"""Thống kê dữ liệu thô của MỘT ngày trong data/raw/dnse.

Chạy từ thư mục gốc: python -m tools.inspect_raw              (hôm nay, giờ VN)
                      python -m tools.inspect_raw 2026-10-16   (ngày cụ thể)
"""
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
selected_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(VIETNAM_TIMEZONE).strftime("%Y-%m-%d")
file_paths = sorted((Path("data/raw/dnse") / selected_date).glob("*.jsonl"))
print(f"Ngày: {selected_date} | đọc {len(file_paths)} tệp")

message_counts, missing_event_time_counts = Counter(), Counter()
symbols_by_topic = defaultdict(set)
first_time, last_time = {}, {}
callback_delays = []            # giây, từ lúc SDK nhận đến lúc collector xử lý (cùng một đồng hồ nên không bị lệch giờ)
invalid_line_count = 0

for file_path in file_paths:
    with file_path.open(encoding="utf-8") as file_handle:
        for line in file_handle:
            try:
                record = json.loads(line)
                topic = record["topic"]
                received_time = datetime.fromisoformat(record["collector_received_timestamp_utc"])
            except (ValueError, KeyError, TypeError):
                invalid_line_count += 1
                continue
            message_counts[topic] += 1
            symbols_by_topic[topic].add(record.get("symbol"))
            first_time.setdefault(topic, received_time)
            last_time[topic] = received_time
            if record.get("event_timestamp_utc") is None:
                missing_event_time_counts[topic] += 1
            if record.get("sdk_received_timestamp_utc"):
                sdk_time = datetime.fromisoformat(record["sdk_received_timestamp_utc"])
                callback_delays.append((received_time - sdk_time).total_seconds())

print(f"Dòng hỏng hoặc thiếu thời gian nhận: {invalid_line_count}\n")
for topic, count in message_counts.most_common():
    start = first_time[topic].astimezone(VIETNAM_TIMEZONE).strftime("%H:%M:%S")
    end = last_time[topic].astimezone(VIETNAM_TIMEZONE).strftime("%H:%M:%S")
    print(f"{topic:<16}{count:>8} message | {len(symbols_by_topic[topic]):>2} mã | từ {start} đến {end} (giờ VN)"
          f" | thiếu event_timestamp_utc: {missing_event_time_counts[topic]}")
if callback_delays:
    print(f"\nĐộ trễ SDK -> collector: trung bình {sum(callback_delays) / len(callback_delays) * 1000:.1f} ms,"
          f" lớn nhất {max(callback_delays) * 1000:.1f} ms")