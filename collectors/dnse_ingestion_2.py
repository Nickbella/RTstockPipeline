import argparse, asyncio, dataclasses, itertools, logging, os, time
from collections import Counter
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dnse import TradingClient

from utils.datetime_solving import safe_utc_iso
from utils.queued_sink import QueuedSink
from utils.raw_writer import RawWriter

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dnse")

API_KEY, API_SECRET = os.getenv("DNSE_API_KEY"), os.getenv("DNSE_API_SECRET")
WS_URL = os.getenv("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
ENCODING = os.getenv("DNSE_ENCODING", "msgpack")
BOARD_ID = "G1"
SILENCE_LIMIT = 180                      
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

TOPICS = {
    "trade":          ("subscribe_trades", "on_trade", {"board_id": BOARD_ID}, "time"),
    "ohlc_1":         ("subscribe_ohlc", "on_ohlc", {"resolution": "1"}, "time"),
    "quote":          ("subscribe_quotes", "on_quote", {"board_id": BOARD_ID}, None),
    "foreign":        ("subscribe_foreign_trading", "on_trade", {"board_id": BOARD_ID}, None),
    "expected_price": ("subscribe_expected_price", "on_expected_price", {"board_id": BOARD_ID}, None),
}

writer = RawWriter("dnse")
sink = QueuedSink(writer)
stats, last, seq = Counter(), {"t": time.monotonic()}, itertools.count(1)

# Chuyển đối tượng SDK trả về thành dict để ghi JSON
def to_dict(obj):
    if isinstance(obj, dict):
        return obj
    for name in ("model_dump", "dict"):
        fn = getattr(obj, name, None) # Lấy phương thức model_dump hoặc dict nếu có
        if callable(fn):
            return fn()
    if dataclasses.is_dataclass(obj): # Nếu là dataclass thì chuyển sang dict
        return dataclasses.asdict(obj)
    return {k: v for k, v in vars(obj).items() if not k.startswith("_")}

# Lấy danh sách mã chứng khoán VN30
def load_symbols(limit=None):
    p = Path("config/vn30.txt")
    syms = []
    for line in (p.read_text(encoding="utf-8")).splitlines(): # Đọc file config/vn30.txt, tách từng dòng
        syms += line.split("#")[0].replace(",", " ").upper().split() 
    syms = list(dict.fromkeys(syms))   
    return syms[:limit] if limit else syms 

# Tạo hàm xử lý message cho từng topic, ghi vào queue để ghi vào file JSONL
def make_handler(topic, ts_field):
    def handler(msg):
        try:
            last["t"] = time.monotonic()
            d = to_dict(msg)
            if stats[topic] == 0:
                log.info("Mẫu %s: %s", topic, d)
            sink.put(writer.build_record(
                topic, d, d.get("symbol"),
                safe_utc_iso(d.get(ts_field)) if ts_field else None, next(seq)))
            stats[topic] += 1
        except Exception:
            log.exception("Lỗi xử lý message %s", topic)
    return handler

# Thời gian giao dịch liên tục, trừ phiên ATO và ATC, nghỉ trưa và cuối tuần 
def expecting_data(now):
    t = now.time()
    return now.weekday() < 5 and (dtime(9, 15) <= t < dtime(11, 30) or dtime(13, 0) <= t < dtime(14, 30))

# In ra thống kê sau mỗi 30 giây 
async def stats_loop():
    while True:
        await asyncio.sleep(30)
        log.info("Nhận %s | đã ghi %d | bỏ %d | queue %d | im lặng %.0fs", dict(stats),
                 sink.written, sink.dropped, sink.q.qsize(), time.monotonic() - last["t"])

# Kết nối và chạy trong phiên giao dịch, đăng ký các topic và mã chứng khoán, ghi dữ liệu vào file JSONL
async def run_session(topics, symbols, stop_at):
    client = TradingClient(api_key=API_KEY, api_secret=API_SECRET, base_url=WS_URL, encoding=ENCODING)
    try:
        await client.connect()
        last["t"] = time.monotonic()
        for t in topics:
            method, cb, extra, ts_field = TOPICS[t]
            syms = symbols + ["VN30"] if t == "ohlc_1" else symbols
            await getattr(client, method)(syms, **{cb: make_handler(t, ts_field)},
                                          encoding=ENCODING, **extra)
        log.info("Đã đăng ký %d mã, topic: %s", len(symbols), ", ".join(topics))

        # Chờ đến khi hết phiên giao dịch hoặc hết thời gian quy định, nếu không có message trong 180 giây thì báo lỗi
        while True:
            await asyncio.sleep(5)
            now = datetime.now(VN_TZ)
            if stop_at and now >= stop_at:
                return
            if expecting_data(now) and time.monotonic() - last["t"] > SILENCE_LIMIT:
                raise RuntimeError(f"Không có message trong {SILENCE_LIMIT}s")
    finally:
        # Ngắt kết nối client, nếu lỗi thì log cảnh báo
        try:
            await client.disconnect()        
        except Exception as e:
            log.warning("Lỗi khi ngắt kết nối: %s", e)


async def main(args):
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    if bad := [t for t in topics if t not in TOPICS]:
        raise SystemExit(f"Topic không hợp lệ: {bad}. Hợp lệ: {list(TOPICS)}")
    symbols = load_symbols(args.limit)
    now = datetime.now(VN_TZ)
    stop_at = None
    if args.duration:
        stop_at = now + timedelta(seconds=args.duration)
    elif args.until:
        h, m = map(int, args.until.split(":"))
        stop_at = now.replace(hour=h, minute=m, second=0, microsecond=0)

    sink.start()
    stats_task = asyncio.create_task(stats_loop())
    backoff = 1
    try:
        while not (stop_at and datetime.now(VN_TZ) >= stop_at):
            t0 = time.monotonic()
            try:
                await run_session(topics, symbols, stop_at)
            except Exception as e:
                log.error("Lỗi phiên kết nối: %s. Thử lại sau %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = 1 if time.monotonic() - t0 > 60 else min(backoff * 2, 60)
    finally:
        stats_task.cancel()
        try:
            await sink.stop()                         
        except Exception as e:
            log.warning("Lỗi khi dừng: %s", e)
        log.info("Kết thúc. Đã ghi %d | bỏ %d", sink.written, sink.dropped)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", default="trade,ohlc_1", help=f"Hợp lệ: {','.join(TOPICS)}")
    try:
        asyncio.run(main(ap.parse_args()))
    except KeyboardInterrupt:
        log.info("Dừng theo yêu cầu")