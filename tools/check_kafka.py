"""Đối chiếu số bản ghi hôm nay giữa tệp JSONL và Kafka. Chạy từ thư mục gốc: python -m tools.check_kafka --since 09:30

Chỉ tính bản ghi có thời điểm collector nhận từ mốc --since (giờ VN) trở đi, áp dụng cho cả hai phía.
Chạy khi collector đã dừng để số liệu không đổi.
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

TOPICS = ["trade", "ohlc_1_minute", "quote", "foreign_trading", "expected_price"]
IDLE_SECONDS = 10                       # dừng đọc Kafka khi không có message mới trong ngần ấy giây

argument_parser = argparse.ArgumentParser()
argument_parser.add_argument("--since", default="00:00", help="Mốc HH:MM giờ VN hôm nay")
argument_parser.add_argument("--servers", default="localhost:9092")
arguments = argument_parser.parse_args()

today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
hour, minute = map(int, arguments.since.split(":"))
since_time = today.replace(hour=hour, minute=minute, second=0, microsecond=0)


def read_file_lines():
    for file_path in sorted((Path("data/raw/dnse") / today.strftime("%Y-%m-%d")).glob("*.jsonl")):
        yield from file_path.open(encoding="utf-8")


def read_kafka_values():
    consumer = Consumer({"bootstrap.servers": arguments.servers, "group.id": f"check-kafka-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([to_kafka_topic_name(topic) for topic in TOPICS])
    last_message_time = time.monotonic()
    while time.monotonic() - last_message_time < IDLE_SECONDS:
        message = consumer.poll(1.0)
        if message is not None and not message.error():
            last_message_time = time.monotonic()
            yield message.value()
    consumer.close()


def count_records(raw_records):
    counts = Counter()
    for raw_record in raw_records:
        try:
            record = json.loads(raw_record)
            if datetime.fromisoformat(record["collector_received_timestamp_utc"]) >= since_time:
                counts[record["topic"]] += 1
        except (ValueError, KeyError, TypeError):
            continue
    return counts


file_counts, kafka_counts = count_records(read_file_lines()), count_records(read_kafka_values())
print(f"{'topic':<18}{'tệp':>10}{'kafka':>10}{'chênh lệch':>12}")
for topic in TOPICS:
    print(f"{topic:<18}{file_counts[topic]:>10}{kafka_counts[topic]:>10}{kafka_counts[topic] - file_counts[topic]:>12}")
if file_counts and not kafka_counts:
    print("Cảnh báo: không đọc được message nào từ Kafka (kiểm tra container kafka và tham số --servers)")