"""Consumer đầu tiên của pipeline: đọc các topic Kafka, đo độ trễ nội bộ và thông lượng theo từng topic.

Độ trễ nội bộ = giờ hiện tại - thời điểm collector tạo bản ghi (collector_received_timestamp_utc),
hoặc replay_timestamp_utc với dữ liệu phát lại. Hai mốc cùng một máy nên KHÔNG chịu ảnh hưởng của độ lệch
đồng hồ. Gồm: queue của collector, gom lô, Kafka và consumer.

Chạy từ thư mục gốc dự án (bật consumer TRƯỚC, rồi mới chạy collector hoặc replay):
    python -m consumers.latency_monitor --duration 60
    python -m consumers.latency_monitor --topic-prefix replay-dnse- --duration 60
Dùng --from-beginning để đọc lại từ đầu topic (khi đó "độ trễ" là tuổi của message, không phải độ trễ thật).
"""
import argparse
import json
import statistics
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer

from utils.kafka_writer import to_kafka_topic_name


def read_reference_time_text(record):
    if record.get("is_replay"):
        return record.get("replay_timestamp_utc")
    return record.get("collector_received_timestamp_utc") or record.get("recv_ts")


def describe_latencies(latencies_in_milliseconds):
    sorted_latencies = sorted(latencies_in_milliseconds)
    return (f"trung vị {statistics.median(sorted_latencies):8.1f} ms | "
            f"p95 {sorted_latencies[int(len(sorted_latencies) * 0.95)]:8.1f} ms | "
            f"lớn nhất {sorted_latencies[-1]:8.1f} ms")


def print_window_report(window_latencies, window_seconds, elapsed_seconds):
    if not window_latencies:
        print(f"[{elapsed_seconds:5.0f}s] không có message mới")
        return
    for topic, latencies in sorted(window_latencies.items()):
        print(f"[{elapsed_seconds:5.0f}s] {topic:<24} n={len(latencies):>6} ({len(latencies) / window_seconds:7.1f} msg/s) "
              f"| {describe_latencies(latencies)}")


def main(arguments):
    topic_names = [to_kafka_topic_name(name.strip(), arguments.topic_prefix)
                   for name in arguments.topics.split(",") if name.strip()]
    consumer = Consumer({
        "bootstrap.servers": arguments.kafka_servers,
        "group.id": arguments.group_id or f"latency-monitor-{uuid.uuid4()}",
        "auto.offset.reset": "earliest" if arguments.from_beginning else "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topic_names)
    print(f"Đang đọc: {topic_names} (Ctrl+C để dừng)")

    window_latencies, all_latencies = defaultdict(list), defaultdict(list)
    invalid_message_count = 0
    start_time = window_start_time = time.monotonic()
    try:
        while not (arguments.duration and time.monotonic() - start_time >= arguments.duration):
            message = consumer.poll(1.0)
            if message is not None and not message.error():
                try:
                    record = json.loads(message.value())
                    reference_time = datetime.fromisoformat(read_reference_time_text(record))
                    latency_ms = (datetime.now(timezone.utc) - reference_time).total_seconds() * 1000
                except (ValueError, TypeError):
                    invalid_message_count += 1
                else:
                    window_latencies[message.topic()].append(latency_ms)
                    all_latencies[message.topic()].append(latency_ms)
            window_seconds = time.monotonic() - window_start_time
            if window_seconds >= arguments.report_seconds:
                print_window_report(window_latencies, window_seconds, time.monotonic() - start_time)
                window_latencies, window_start_time = defaultdict(list), time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("\n=== Tổng kết ===")
    for topic, latencies in sorted(all_latencies.items()):
        print(f"{topic:<24} n={len(latencies):>7} | {describe_latencies(latencies)}")
    if not all_latencies:
        print("Không nhận được message nào (kiểm tra Kafka, tên topic, tiền tố --topic-prefix)")
    if invalid_message_count:
        print(f"Message không đọc được: {invalid_message_count}")


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--kafka-servers", default="localhost:9092")
    argument_parser.add_argument("--topics", default="trade,ohlc_1_minute,quote",
                                 help="Tên topic của collector (không có tiền tố)")
    argument_parser.add_argument("--topic-prefix", default="dnse-", help="dnse- (dữ liệu thật) hoặc replay-dnse-")
    argument_parser.add_argument("--duration", type=int, default=0, help="Tự dừng sau N giây (0 = đến khi Ctrl+C)")
    argument_parser.add_argument("--report-seconds", type=int, default=10)
    argument_parser.add_argument("--group-id", help="Mặc định: nhóm dùng một lần, không ảnh hưởng consumer khác")
    argument_parser.add_argument("--from-beginning", action="store_true")
    main(argument_parser.parse_args())