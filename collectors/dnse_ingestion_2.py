import argparse
import asyncio
import dataclasses
import itertools
import logging
import os
import time
from collections import Counter
from datetime import datetime, time as time_of_day_type, timedelta, timezone
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

API_KEY = os.getenv("DNSE_API_KEY")
API_SECRET = os.getenv("DNSE_API_SECRET")

WEBSOCKET_URL = os.getenv("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
MESSAGE_ENCODING = os.getenv("DNSE_ENCODING", "msgpack")
BOARD_ID = "G1"
SILENCE_LIMIT_SECONDS = 180             # giây im lặng trong giờ khớp lệnh thì nối lại
VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
SDK_RECEIVED_FIELD_NAME = "receivedAt"  # trường trong payload: thời điểm SDK nhận message (epoch giây)

# topic: (tên hàm subscribe, tên tham số callback, tham số riêng, tên trường thời gian của sàn hoặc None)
TOPIC_CONFIGURATIONS = {
    "trade":           ("subscribe_trades", "on_trade", {"board_id": BOARD_ID}, "time"),
    "ohlc_1_minute":   ("subscribe_ohlc", "on_ohlc", {"resolution": "1"}, "time"),
    "quote":           ("subscribe_quotes", "on_quote", {"board_id": BOARD_ID}, "time"),
    "foreign_trading": ("subscribe_foreign_trading", "on_trade", {"board_id": BOARD_ID}, "transactTime"),
    "expected_price":  ("subscribe_expected_price", "on_expected_price", {"board_id": BOARD_ID}, "time"),
}

def convert_foreign_trading_time(time_text):
    """transactTime của foreign_trading là giờ UTC trong ngày dạng HHMMSSmmm (không có ngày)."""
    return time_of_day_to_utc_iso_string(time_text, datetime.now(timezone.utc))


# Topic nào không có trong bảng này thì trường thời gian là chuỗi hoặc epoch thông thường.
EVENT_TIME_CONVERTERS = {"foreign_trading": convert_foreign_trading_time}

raw_writer = RawWriter("dnse")
record_sinks = {"file": QueuedSink(raw_writer)}    # sink "kafka" được thêm trong main nếu bật --kafka-servers
message_counts = Counter()
connection_state = {"last_message_time": time.monotonic()}
sequence_counter = itertools.count(1)   # số thứ tự tăng dần, dùng để dò mất hoặc lệch thứ tự


def convert_to_dictionary(message_object):
    """Đổi đối tượng SDK trả về thành dict để ghi JSON."""
    if isinstance(message_object, dict):
        return message_object
    for method_name in ("model_dump", "dict"):
        method = getattr(message_object, method_name, None)
        if callable(method):
            return method()
    if dataclasses.is_dataclass(message_object):
        return dataclasses.asdict(message_object)
    return {key: value for key, value in vars(message_object).items() if not key.startswith("_")}


def load_symbols(maximum_symbols=None):
    """Đọc mã từ config/vn30.txt: bỏ chú thích (#), dấu phẩy, mã trùng."""
    symbols_file_path = Path("config/vn30.txt")
    file_text = symbols_file_path.read_text(encoding="utf-8") if symbols_file_path.exists() else "HPG VCB FPT"
    symbols = []
    for line in file_text.splitlines():
        symbols += line.split("#")[0].replace(",", " ").upper().split()
    symbols = list(dict.fromkeys(symbols))
    return symbols[:maximum_symbols] if maximum_symbols else symbols


def create_message_handler(topic_name, timestamp_field_name):
    """PRODUCER: chạy trong event loop, chỉ tạo bản ghi rồi đẩy vào các queue, không ghi tệp, không gọi Kafka."""
    convert_event_time = EVENT_TIME_CONVERTERS.get(topic_name, safe_to_utc_iso_string)

    def handle_message(message):
        try:
            connection_state["last_message_time"] = time.monotonic()
            message_dictionary = convert_to_dictionary(message)
            if message_counts[topic_name] == 0:
                logger.info("Mẫu %s: %s", topic_name, message_dictionary)
            keyed_record = raw_writer.build_record(
                topic=topic_name,
                payload=message_dictionary,
                symbol=message_dictionary.get("symbol"),
                event_timestamp_utc=(convert_event_time(message_dictionary.get(timestamp_field_name))
                                     if timestamp_field_name else None),
                sdk_received_timestamp_utc=safe_to_utc_iso_string(message_dictionary.get(SDK_RECEIVED_FIELD_NAME)),
                sequence_number=next(sequence_counter),
            )
            for record_sink in record_sinks.values():     # cùng một bản ghi cho mọi đích, chỉ đọc nên an toàn
                record_sink.enqueue_record(keyed_record)
            message_counts[topic_name] += 1
        except Exception:
            logger.exception("Lỗi xử lý message %s", topic_name)
    return handle_message


def is_continuous_trading_time(current_time):
    """Giờ khớp lệnh liên tục (không tính ATO, nghỉ trưa, ATC). Chưa xử lý ngày lễ."""
    time_of_day = current_time.time()
    in_morning = time_of_day_type(9, 15) <= time_of_day < time_of_day_type(11, 30)
    in_afternoon = time_of_day_type(13, 0) <= time_of_day < time_of_day_type(14, 30)
    return current_time.weekday() < 5 and (in_morning or in_afternoon)


def describe_sinks():
    """Một dòng tóm tắt cho mỗi đích ghi: đã ghi, bỏ, queue (và số gửi thành công/lỗi nếu là Kafka)."""
    descriptions = []
    for sink_name, record_sink in record_sinks.items():
        text = (f"{sink_name}: ghi {record_sink.written_record_count} bỏ {record_sink.dropped_record_count} "
                f"queue {record_sink.record_queue.qsize()}")
        delivered = getattr(record_sink.batch_writer, "delivered_message_count", None)
        if delivered is not None:
            text += f" xác nhận {delivered} lỗi {record_sink.batch_writer.failed_message_count}"
        descriptions.append(text)
    return " | ".join(descriptions)


async def log_statistics_periodically():
    while True:
        await asyncio.sleep(30)
        logger.info("Nhận %s | %s | im lặng %.0fs", dict(message_counts), describe_sinks(),
                    time.monotonic() - connection_state["last_message_time"])


async def run_connection_session(topic_names, symbols, stop_time):
    dnse_client = TradingClient(api_key=API_KEY, api_secret=API_SECRET,
                                base_url=WEBSOCKET_URL, encoding=MESSAGE_ENCODING)
    try:
        await dnse_client.connect()
        connection_state["last_message_time"] = time.monotonic()
        for topic_name in topic_names:
            subscribe_method_name, callback_parameter_name, extra_arguments, timestamp_field_name = \
                TOPIC_CONFIGURATIONS[topic_name]
            subscribed_symbols = symbols + ["VN30"] if topic_name == "ohlc_1_minute" else symbols
            await getattr(dnse_client, subscribe_method_name)(
                subscribed_symbols,
                **{callback_parameter_name: create_message_handler(topic_name, timestamp_field_name)},
                encoding=MESSAGE_ENCODING, **extra_arguments)
        logger.info("Đã đăng ký %d mã, topic: %s", len(symbols), ", ".join(topic_names))
        while True:
            await asyncio.sleep(5)
            current_time = datetime.now(VIETNAM_TIMEZONE)
            if stop_time and current_time >= stop_time:
                return
            silent_seconds = time.monotonic() - connection_state["last_message_time"]
            if is_continuous_trading_time(current_time) and silent_seconds > SILENCE_LIMIT_SECONDS:
                raise RuntimeError(f"Không có message trong {SILENCE_LIMIT_SECONDS}s")
    finally:
        try:
            await dnse_client.disconnect()      # dừng producer trước khi dừng các consumer
        except Exception as error:
            logger.warning("Lỗi khi ngắt kết nối: %s", error)


def add_kafka_sink(kafka_servers, topic_names):
    """Thêm sink Kafka. Kafka lỗi chỉ cảnh báo, không bao giờ làm collector dừng hay ngừng ghi tệp."""
    from utils.kafka_writer import KafkaWriter, create_missing_topics   # nhập muộn: không cần cài confluent-kafka nếu không dùng Kafka
    try:
        created_topics = create_missing_topics(kafka_servers, topic_names)
        logger.info("Kafka %s sẵn sàng. Topic mới tạo: %s", kafka_servers, created_topics or "không có")
    except Exception as error:
        logger.warning("Chưa kiểm tra/tạo được topic Kafka (%s). Vẫn tiếp tục, producer sẽ tự thử kết nối lại", error)
    record_sinks["kafka"] = QueuedSink(KafkaWriter(kafka_servers))


async def main(arguments):
    topic_names = [name.strip() for name in arguments.topics.split(",") if name.strip()]
    invalid_topics = [name for name in topic_names if name not in TOPIC_CONFIGURATIONS]
    if invalid_topics:
        raise SystemExit(f"Topic không hợp lệ: {invalid_topics}. Hợp lệ: {list(TOPIC_CONFIGURATIONS)}")
    symbols = load_symbols(arguments.limit)
    if arguments.kafka_servers:
        add_kafka_sink(arguments.kafka_servers, topic_names)

    current_time = datetime.now(VIETNAM_TIMEZONE)
    stop_time = None
    if arguments.duration:
        stop_time = current_time + timedelta(seconds=arguments.duration)
    elif arguments.until:
        hour, minute = map(int, arguments.until.split(":"))
        stop_time = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)

    for record_sink in record_sinks.values():
        record_sink.start()
    statistics_task = asyncio.create_task(log_statistics_periodically())
    retry_delay_seconds = 1
    try:
        while not (stop_time and datetime.now(VIETNAM_TIMEZONE) >= stop_time):
            session_start_time = time.monotonic()
            try:
                await run_connection_session(topic_names, symbols, stop_time)
            except Exception as error:
                logger.error("Lỗi phiên kết nối: %s. Thử lại sau %ss", error, retry_delay_seconds)
                await asyncio.sleep(retry_delay_seconds)
                session_was_stable = time.monotonic() - session_start_time > 60
                retry_delay_seconds = 1 if session_was_stable else min(retry_delay_seconds * 2, 60)
    finally:
        statistics_task.cancel()
        for record_sink in record_sinks.values():
            record_sink.stop()                  # poison pill: ghi nốt phần còn trong queue
        logger.info("Kết thúc. %s", describe_sinks())


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--until", help="Tự dừng lúc HH:MM giờ VN, ví dụ 15:05")
    argument_parser.add_argument("--duration", type=int, help="Tự dừng sau N giây (để thử nhanh)")
    argument_parser.add_argument("--topics", default="trade,ohlc_1_minute",
                                 help=f"Hợp lệ: {','.join(TOPIC_CONFIGURATIONS)}")
    argument_parser.add_argument("--limit", type=int, help="Chỉ lấy N mã đầu trong config/vn30.txt")
    argument_parser.add_argument("--kafka-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                                 help="Địa chỉ Kafka, ví dụ localhost:9092. Bỏ trống thì chỉ ghi tệp")
    try:
        asyncio.run(main(argument_parser.parse_args()))
    except KeyboardInterrupt:
        logger.info("Dừng theo yêu cầu")