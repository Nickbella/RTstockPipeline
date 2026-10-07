import argparse
import asyncio
import dataclasses
import logging
import os
import time
from collections import Counter
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dnse import TradingClient

from utils.datetime_solving import safe_utc_iso
from utils.raw_writer import RawWriter

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dnse")

API_KEY = os.getenv("DNSE_API_KEY")
API_SECRET = os.getenv("DNSE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise SystemExit("Thiếu DNSE_API_KEY hoặc DNSE_API_SECRET trong .env")

WS_URL = os.getenv("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
ENCODING = os.getenv("DNSE_ENCODING", "msgpack")   # "json" hoặc "msgpack"
BOARD_ID = "G1"                                    # giữ như ví dụ trade.py
SYMBOLS = ["HPG", "VCB", "FPT"]
OHLC_SYMBOLS = SYMBOLS + ["VN30"]
SILENCE_LIMIT = 180                                # giây im lặng trong giờ giao dịch thì nối lại
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Điền sau khi xem "Mẫu ..." trong nhật ký (bước 2). None = chưa biết.
TS_FIELDS = {"trade": "time", "ohlc_1": "time"}

writer = RawWriter("dnse")
stats = Counter()
state = {"last_msg": time.monotonic()}
seen = set()


def to_dict(obj):
    """Đổi đối tượng SDK trả về thành dict để ghi JSON."""
    if isinstance(obj, dict):
        return obj
    for name in ("model_dump", "dict"):
        fn = getattr(obj, name, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in vars(obj).items() if not k.startswith("_")}
    return {"repr": repr(obj)}


def make_handler(topic):
    def handler(msg):
        try:
            state["last_msg"] = time.monotonic()
            d = to_dict(msg)
            if topic not in seen:
                seen.add(topic)
                log.info("Mẫu %s: %s", topic, d)
            field = TS_FIELDS.get(topic)
            writer.write(
                topic=topic,
                payload=d,
                symbol=d.get("symbol"),
                event_ts=safe_utc_iso(d.get(field)) if field else None,
            )
            stats[topic] += 1
        except Exception:
            stats["loi_xu_ly"] += 1
            log.exception("Lỗi xử lý message %s", topic)
    return handler


def expecting_data(now):
    """Đang trong giờ khớp lệnh (không tính nghỉ trưa). Chưa xử lý ngày lễ."""
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 0) <= t < dtime(11, 30) or dtime(13, 0) <= t < dtime(14, 45)


def parse_until(s):
    if not s:
        return None
    h, m = map(int, s.split(":"))
    return datetime.now(VN_TZ).replace(hour=h, minute=m, second=0, microsecond=0)


async def stats_loop():
    while True:
        await asyncio.sleep(30)
        silent = time.monotonic() - state["last_msg"]
        log.info("Số message: %s | im lặng %.0fs", dict(stats), silent)


async def run_session(stop_at):
    client = TradingClient(api_key=API_KEY, api_secret=API_SECRET,
                           base_url=WS_URL, encoding=ENCODING)
    try:
        log.info("Đang kết nối...")
        await client.connect()
        state["last_msg"] = time.monotonic()
        log.info("Đã kết nối, đang đăng ký kênh dữ liệu")
        await client.subscribe_trades(SYMBOLS, on_trade=make_handler("trade"),
                                      encoding=ENCODING, board_id=BOARD_ID)
        await client.subscribe_ohlc(OHLC_SYMBOLS, resolution="1",
                                    on_ohlc=make_handler("ohlc_1"), encoding=ENCODING)
        log.info("Đã đăng ký: trade + ohlc 1 phút")

        while True:
            await asyncio.sleep(5)
            now = datetime.now(VN_TZ)
            if stop_at and now >= stop_at:
                return True
            if expecting_data(now) and time.monotonic() - state["last_msg"] > SILENCE_LIMIT:
                raise RuntimeError(f"Không có message trong {SILENCE_LIMIT}s")
    finally:
        try:
            await client.disconnect()
        except Exception as e:
            log.warning("Lỗi khi ngắt kết nối: %s", e)


async def main(until):
    stop_at = parse_until(until)
    stats_task = asyncio.create_task(stats_loop())
    backoff = 1
    try:
        while True:
            t0 = time.monotonic()
            try:
                if await run_session(stop_at):
                    log.info("Đến giờ dừng, kết thúc")
                    break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("Lỗi phiên kết nối: %s", e)
            if time.monotonic() - t0 > 60:
                backoff = 1                      # phiên chạy ổn định thì reset thời gian chờ
            if stop_at and datetime.now(VN_TZ) >= stop_at:
                break
            log.info("Thử lại sau %ss", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    finally:
        stats_task.cancel()
        writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--until", help="Tự dừng lúc HH:MM giờ VN, ví dụ 15:05")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.until))
    except KeyboardInterrupt:
        log.info("Dừng theo yêu cầu")