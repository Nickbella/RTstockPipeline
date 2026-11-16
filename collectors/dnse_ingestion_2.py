import argparse, asyncio, dataclasses, itertools, logging, os, time
from collections import Counter
from datetime import datetime, time as clock, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dnse import TradingClient

from utils.datetime_solving import safe_to_utc_iso_string, time_of_day_to_utc_iso_string
from utils.queued_sink import QueuedSink
from utils.raw_writer import RawWriter

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dnse")

API_KEY, API_SECRET = os.getenv("DNSE_API_KEY"), os.getenv("DNSE_API_SECRET")
WEBSOCKET_URL = os.getenv("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
ENCODING = os.getenv("DNSE_ENCODING", "msgpack")
BOARD_ID, SILENCE_LIMIT, VN_TZ = "G1", 180, ZoneInfo("Asia/Ho_Chi_Minh")


def foreign_time(text):
    """transactTime là giờ UTC trong ngày dạng HHMMSSmmm (không có ngày)."""
    return time_of_day_to_utc_iso_string(text, datetime.now(timezone.utc))


# topic: (hàm subscribe, tham số callback, tham số riêng, trường thời gian của sàn, hàm đổi giờ)
TOPICS = {
    "trade":           ("subscribe_trades", "on_trade", {"board_id": BOARD_ID}, "time", safe_to_utc_iso_string),
    "ohlc_1_minute":   ("subscribe_ohlc", "on_ohlc", {"resolution": "1"}, "time", safe_to_utc_iso_string),
    "quote":           ("subscribe_quotes", "on_quote", {"board_id": BOARD_ID}, "time", safe_to_utc_iso_string),
    "foreign_trading": ("subscribe_foreign_trading", "on_trade", {"board_id": BOARD_ID}, "transactTime", foreign_time),
    "expected_price":  ("subscribe_expected_price", "on_expected_price", {"board_id": BOARD_ID}, "time", safe_to_utc_iso_string),
}

raw_writer = RawWriter("dnse")
sinks = {"file": QueuedSink(raw_writer)}          # sink "kafka" được thêm trong main nếu có --kafka-servers
counts, last_message = Counter(), {"time": time.monotonic()}
sequence = itertools.count(1)


def to_dict(message):
    if isinstance(message, dict):
        return message
    for method in ("model_dump", "dict"):
        if callable(getattr(message, method, None)):
            return getattr(message, method)()
    if dataclasses.is_dataclass(message):
        return dataclasses.asdict(message)
    return {k: v for k, v in vars(message).items() if not k.startswith("_")}


def load_symbols(limit=None):
    path = Path("config/vn100.txt")
    text = path.read_text(encoding="utf-8") if path.exists() else "HPG VCB FPT"
    symbols = list(dict.fromkeys(s for line in text.splitlines()
                                 for s in line.split("#")[0].replace(",", " ").upper().split()))
    return symbols[:limit] if limit else symbols

def make_handler(topic, time_field, convert_time):
    def handle(message):
        try:
            last_message["time"] = time.monotonic()
            payload = to_dict(message)
            if counts[topic] == 0:
                logger.info("Mẫu %s: %s", topic, payload)
            record = raw_writer.build_record(
                topic=topic, payload=payload, symbol=payload.get("symbol"),
                event_timestamp_utc=convert_time(payload.get(time_field)),
                sdk_received_timestamp_utc=safe_to_utc_iso_string(payload.get("receivedAt")),
                sequence_number=next(sequence))
            for sink in sinks.values():
                sink.enqueue_record(record)
            counts[topic] += 1
        except Exception:
            logger.exception("Lỗi xử lý message %s", topic)
    return handle


def is_continuous_trading(now):
    """Giờ khớp lệnh liên tục (không tính ATO, nghỉ trưa, ATC). Chưa xử lý ngày lễ."""
    t = now.time()
    return now.weekday() < 5 and (clock(9, 15) <= t < clock(11, 30) or clock(13, 0) <= t < clock(14, 30))


def describe_sinks():
    parts = []
    for name, sink in sinks.items():
        text = f"{name}: ghi {sink.written_record_count} bỏ {sink.dropped_record_count} queue {sink.record_queue.qsize()}"
        if hasattr(sink.batch_writer, "delivered_message_count"):
            text += f" xác nhận {sink.batch_writer.delivered_message_count} lỗi {sink.batch_writer.failed_message_count}"
        parts.append(text)
    return " | ".join(parts)


async def log_statistics():
    while True:
        await asyncio.sleep(30)
        logger.info("Nhận %s | %s | im lặng %.0fs", dict(counts), describe_sinks(),
                    time.monotonic() - last_message["time"])


async def run_session(topics, symbols, stop_time):
    client = TradingClient(api_key=API_KEY, api_secret=API_SECRET, base_url=WEBSOCKET_URL, encoding=ENCODING)
    try:
        await client.connect()
        last_message["time"] = time.monotonic()
        for topic in topics:
            subscribe, callback, extra, time_field, convert_time = TOPICS[topic]
            await getattr(client, subscribe)(
                symbols,
                **{callback: make_handler(topic, time_field, convert_time)}, encoding=ENCODING, **extra)
        logger.info("Đã đăng ký %d mã, topic: %s", len(symbols), ", ".join(topics))
        while True:
            await asyncio.sleep(5)
            now = datetime.now(VN_TZ)
            if stop_time and now >= stop_time:
                return
            if is_continuous_trading(now) and time.monotonic() - last_message["time"] > SILENCE_LIMIT:
                raise RuntimeError(f"Không có message trong {SILENCE_LIMIT}s")
    finally:
        try:
            await client.disconnect()
        except Exception as error:
            logger.warning("Lỗi khi ngắt kết nối: %s", error)


async def main(args):
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    invalid = [t for t in topics if t not in TOPICS]
    if invalid:
        raise SystemExit(f"Topic không hợp lệ: {invalid}. Hợp lệ: {list(TOPICS)}")
    symbols = load_symbols(args.limit)

    if args.kafka_servers:
        from utils.kafka_writer import KafkaWriter, create_missing_topics
        try:
            logger.info("Kafka sẵn sàng. Topic mới tạo: %s", create_missing_topics(args.kafka_servers, topics) or "không có")
        except Exception as error:
            logger.warning("Chưa kiểm tra/tạo được topic Kafka (%s). Producer sẽ tự thử lại", error)
        sinks["kafka"] = QueuedSink(KafkaWriter(args.kafka_servers))
    else:
        logger.warning("Không có --kafka-servers/KAFKA_BOOTSTRAP_SERVERS: CHỈ ghi tệp, không gửi Kafka")

    now = datetime.now(VN_TZ)
    stop_time = None
    if args.duration:
        stop_time = now + timedelta(seconds=args.duration)
    elif args.until:
        hour, minute = map(int, args.until.split(":"))
        stop_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    for sink in sinks.values():
        sink.start()
    statistics_task = asyncio.create_task(log_statistics())
    delay = 1
    try:
        while not (stop_time and datetime.now(VN_TZ) >= stop_time):
            started = time.monotonic()
            try:
                await run_session(topics, symbols, stop_time)
            except Exception as error:
                logger.error("Lỗi phiên kết nối: %s. Thử lại sau %ss", error, delay)
                await asyncio.sleep(delay)
                delay = 1 if time.monotonic() - started > 60 else min(delay * 2, 60)
    finally:
        statistics_task.cancel()
        for sink in sinks.values():
            sink.stop()                         # ghi nốt phần còn trong queue
        logger.info("Kết thúc. %s", describe_sinks())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--until", help="Tự dừng lúc HH:MM (giờ VN)")
    parser.add_argument("--duration", type=int, help="Tự dừng sau N giây")
    parser.add_argument("--topics", default=",".join(TOPICS), help=f"Hợp lệ: {','.join(TOPICS)}")
    parser.add_argument("--limit", type=int, help="Chỉ lấy N mã đầu trong file mã")
    parser.add_argument("--kafka-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"), help="Địa chỉ Kafka")
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        logger.info("Dừng theo yêu cầu")