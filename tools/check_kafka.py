"""Đếm message trên Kafka và đối chiếu với tệp JSONL của hôm nay.

Chạy từ thư mục gốc: python -m tools.check_kafka --since 09:30
Chỉ tính bản ghi có thời điểm collector nhận từ mốc --since (giờ VN, hôm nay) trở đi, áp dụng cho cả hai phía
để so sánh công bằng. Chạy khi collector đã dừng để số liệu không đổi.
"""
import argparse
import json
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer

from utils.kafka_writer import to_kafka_topic_name

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
RAW_DATA_DIRECTORY = Path("data/raw/dnse")


def read_received_time(record):
    """Đọc thời điểm collector nhận (khóa mới, hoặc khóa cũ recv_ts của dữ liệu thu trước đây)."""
    text = record.get("collector_received_timestamp_utc") or record.get("recv_ts")
    return datetime.fromisoformat(text)


def count_file_records(topic_names, since_time):
    counts = Counter()
    today_directory = RAW_DATA_DIRECTORY / datetime.now(VIETNAM_TIMEZONE).strftime("%Y-%m-%d")
    for file_path in sorted(today_directory.glob("*.jsonl")):
        with file_path.open(encoding="utf-8") as file_handle:
            for line in file_handle:
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                kafka_topic_name = to_kafka_topic_name(record.get("topic", ""))
                if kafka_topic_name in topic_names and read_received_time(record) >= since_time:
                    counts[kafka_topic_name] += 1
    return counts


def count_kafka_messages(bootstrap_servers, topic_names, since_time, idle_seconds):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"check-kafka-{uuid.uuid4()}",     # nhóm dùng một lần, không ảnh hưởng consumer thật
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(sorted(topic_names))
    counts, last_message_time = Counter(), time.monotonic()
    while time.monotonic() - last_message_time < idle_seconds:
        message = consumer.poll(1.0)
        if message is None or message.error():
            continue
        last_message_time = time.monotonic()
        try:
            record = json.loads(message.value())
        except ValueError:
            continue
        if read_received_time(record) >= since_time:
            counts[message.topic()] += 1
    consumer.close()
    return counts


def main(arguments):
    hour, minute = map(int, arguments.since.split(":"))
    since_time = datetime.now(VIETNAM_TIMEZONE).replace(hour=hour, minute=minute, second=0, microsecond=0)
    topic_names = {to_kafka_topic_name(name.strip()) for name in arguments.topics.split(",") if name.strip()}

    file_counts = count_file_records(topic_names, since_time)
    kafka_counts = count_kafka_messages(arguments.servers, topic_names, since_time, arguments.idle_seconds)

    print(f"{'topic Kafka':<24}{'tệp':>10}{'kafka':>10}{'chênh lệch':>12}")
    for topic_name in sorted(topic_names):
        difference = kafka_counts[topic_name] - file_counts[topic_name]
        print(f"{topic_name:<24}{file_counts[topic_name]:>10}{kafka_counts[topic_name]:>10}{difference:>12}")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--since", default="00:00", help="Mốc HH:MM giờ VN hôm nay, mặc định 00:00")
    argument_parser.add_argument("--servers", default="localhost:9092")
    argument_parser.add_argument("--topics", default="trade,ohlc_1_minute,quote",
                                 help="Tên topic của collector (không có tiền tố dnse-)")
    argument_parser.add_argument("--idle-seconds", type=int, default=10,
                                 help="Dừng đọc khi không có message mới trong N giây")
    main(argument_parser.parse_args())