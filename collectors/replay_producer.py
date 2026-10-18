"""Phát lại dữ liệu thô đã thu (data/raw/dnse/<ngày>) vào Kafka, để phát triển và demo khi sàn đóng cửa.

Giữ nhịp thời gian gốc theo collector_received_timestamp_utc, chia cho --speed. Gửi vào các topic RIÊNG
(tiền tố replay-dnse-) để không lẫn với dữ liệu thật; mỗi bản ghi được gắn is_replay và replay_timestamp_utc.

Chạy từ thư mục gốc dự án:
    python -m collectors.replay_producer --date 2026-10-16 --speed 10 --from-time 09:15 --to-time 09:45
    python -m collectors.replay_producer --date 2026-10-16 --speed 0         (nhanh nhất có thể)
"""
import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.kafka_writer import KafkaWriter, create_missing_topics
from utils.record_normalizer import normalize_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("replay")

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_TOPICS = "trade,ohlc_1_minute,quote,foreign_trading,expected_price"
BATCH_SIZE = 500
PROGRESS_LOG_INTERVAL_SECONDS = 5


def parse_time_of_day(time_text, date_text):
    """'09:15' và '2026-10-16' -> datetime có múi giờ Việt Nam."""
    hour, minute = map(int, time_text.split(":"))
    return datetime.strptime(date_text, "%Y-%m-%d").replace(hour=hour, minute=minute, tzinfo=VIETNAM_TIMEZONE)


def read_records(file_paths, topic_names, start_time, end_time):
    """Đọc bản ghi theo thứ tự tệp (thứ tự nhận ban đầu), lọc theo topic và khoảng thời gian."""
    for file_path in file_paths:
        with file_path.open(encoding="utf-8") as file_handle:
            for line in file_handle:
                try:
                    record = normalize_record(json.loads(line))
                except (ValueError, AttributeError):
                    continue
                received_text = record.get("collector_received_timestamp_utc")
                if record.get("topic") not in topic_names or not received_text:
                    continue
                received_time = datetime.fromisoformat(received_text)
                if (start_time and received_time < start_time) or (end_time and received_time >= end_time):
                    continue
                yield received_time, record


def main(arguments):
    topic_names = {name.strip() for name in arguments.topics.split(",") if name.strip()}
    file_paths = sorted((Path("data/raw/dnse") / arguments.date).glob("*.jsonl"))
    if not file_paths:
        raise SystemExit(f"Không có tệp dữ liệu cho ngày {arguments.date} trong data/raw/dnse")
    start_time = parse_time_of_day(arguments.from_time, arguments.date) if arguments.from_time else None
    end_time = parse_time_of_day(arguments.to_time, arguments.date) if arguments.to_time else None

    try:
        created_topics = create_missing_topics(arguments.kafka_servers, sorted(topic_names),
                                               topic_prefix=arguments.topic_prefix)
    except Exception as error:
        raise SystemExit(f"Không kết nối được Kafka ({error}). Kiểm tra container kafka đang chạy (docker ps)")
    logger.info("Đọc %d tệp ngày %s | topic: %s | tốc độ x%s | tiền tố %s | topic mới tạo: %s",
                len(file_paths), arguments.date, sorted(topic_names), arguments.speed,
                arguments.topic_prefix, created_topics or "không có")

    kafka_writer = KafkaWriter(arguments.kafka_servers, topic_prefix=arguments.topic_prefix)
    record_batch, sent_count = [], 0
    virtual_elapsed_seconds, previous_received_time = 0.0, None
    wall_start_time = last_progress_log_time = time.monotonic()
    try:
        for received_time, record in read_records(file_paths, topic_names, start_time, end_time):
            if previous_received_time is not None:
                gap_seconds = (received_time - previous_received_time).total_seconds()
                if gap_seconds > 0:                                     # rút ngắn khoảng nghỉ dài (ví dụ nghỉ trưa)
                    virtual_elapsed_seconds += min(gap_seconds, arguments.max_gap_seconds)
            previous_received_time = received_time

            if arguments.speed > 0:                                     # speed = 0: gửi nhanh nhất có thể
                wait_seconds = virtual_elapsed_seconds / arguments.speed - (time.monotonic() - wall_start_time)
                if wait_seconds > 0:
                    if record_batch:
                        kafka_writer.write_batch(record_batch)
                        record_batch = []
                    time.sleep(wait_seconds)

            record["is_replay"] = True
            record["replay_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
            record_batch.append((None, record))
            sent_count += 1
            if len(record_batch) >= BATCH_SIZE:
                kafka_writer.write_batch(record_batch)
                record_batch = []
            if time.monotonic() - last_progress_log_time >= PROGRESS_LOG_INTERVAL_SECONDS:
                last_progress_log_time = time.monotonic()
                logger.info("Đã phát %d bản ghi | dữ liệu đang ở %s (giờ VN)", sent_count,
                            received_time.astimezone(VIETNAM_TIMEZONE).strftime("%H:%M:%S"))
            if arguments.limit and sent_count >= arguments.limit:
                break
    except KeyboardInterrupt:
        logger.info("Dừng theo yêu cầu")
    finally:
        if record_batch:
            kafka_writer.write_batch(record_batch)
        kafka_writer.close(flush_timeout_seconds=60)
    logger.info("Xong. Đã phát %d | Kafka xác nhận %d | lỗi %d | mất %.1fs",
                sent_count, kafka_writer.delivered_message_count, kafka_writer.failed_message_count,
                time.monotonic() - wall_start_time)


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--date", required=True, help="Ngày dữ liệu, ví dụ 2026-10-16")
    argument_parser.add_argument("--speed", type=float, default=1.0,
                                 help="Hệ số tốc độ: 1 = nhịp gốc, 10 = nhanh gấp 10, 0 = nhanh nhất có thể")
    argument_parser.add_argument("--topics", default=DEFAULT_TOPICS)
    argument_parser.add_argument("--from-time", help="Chỉ phát từ HH:MM (giờ VN, theo giờ collector nhận)")
    argument_parser.add_argument("--to-time", help="Chỉ phát đến trước HH:MM (giờ VN)")
    argument_parser.add_argument("--max-gap-seconds", type=float, default=5.0,
                                 help="Rút ngắn khoảng nghỉ dài giữa hai bản ghi xuống tối đa N giây (trước khi chia tốc độ)")
    argument_parser.add_argument("--limit", type=int, help="Chỉ phát N bản ghi đầu")
    argument_parser.add_argument("--kafka-servers", default="localhost:9092")
    argument_parser.add_argument("--topic-prefix", default="replay-dnse-",
                                 help="Tiền tố topic đích. Dùng dnse- chỉ khi muốn ghi đè vào topic dữ liệu thật")
    main(argument_parser.parse_args())