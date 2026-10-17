"""Thống kê dữ liệu thô trong data/raw/dnse. Chạy từ thư mục gốc: python -m tools.inspect_raw"""
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
# khóa mới -> khóa cũ (dữ liệu thu trước khi đổi tên vẫn đọc được)
LEGACY_KEYS = {"event_timestamp_utc": "event_ts",
               "collector_received_timestamp_utc": "recv_ts",
               "sdk_received_timestamp_utc": "sdk_recv_ts"}


def read_field(record, key):
    return record.get(key) or record.get(LEGACY_KEYS[key])


message_counts, missing_event_counts, symbol_counts = Counter(), Counter(), defaultdict(Counter)
first_time, last_time, invalid_line_count = {}, {}, 0
delays = {"sàn -> SDK": [], "SDK -> collector": [], "sàn -> collector": []}

selected_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(VIETNAM_TIMEZONE).strftime("%Y-%m-%d")
file_paths = sorted((Path("data/raw/dnse") / selected_date).glob("*.jsonl"))
print(f"Ngày: {selected_date} | đọc {len(file_paths)} tệp")

for file_path in file_paths:
    with file_path.open(encoding="utf-8") as file_handle:
        for line in file_handle:
            try:
                record = json.loads(line)
            except ValueError:
                invalid_line_count += 1
                continue
            topic = record.get("topic")
            message_counts[topic] += 1
            symbol_counts[topic][record.get("symbol")] += 1
            collector_time_text = read_field(record, "collector_received_timestamp_utc")
            first_time.setdefault(topic, collector_time_text)
            last_time[topic] = collector_time_text

            event_time_text = read_field(record, "event_timestamp_utc")
            if event_time_text is None:
                missing_event_counts[topic] += 1
            elif topic == "trade":
                event_time = datetime.fromisoformat(event_time_text)
                collector_time = datetime.fromisoformat(collector_time_text)
                delays["sàn -> collector"].append((collector_time - event_time).total_seconds())
                sdk_time_text = read_field(record, "sdk_received_timestamp_utc")
                if sdk_time_text:
                    sdk_time = datetime.fromisoformat(sdk_time_text)
                    delays["sàn -> SDK"].append((sdk_time - event_time).total_seconds())
                    delays["SDK -> collector"].append((collector_time - sdk_time).total_seconds())

print("Dòng hỏng:", invalid_line_count)
for topic, count in message_counts.most_common():
    start = datetime.fromisoformat(first_time[topic]).astimezone(VIETNAM_TIMEZONE).strftime("%H:%M:%S")
    end = datetime.fromisoformat(last_time[topic]).astimezone(VIETNAM_TIMEZONE).strftime("%H:%M:%S")
    print(f"\n=== {topic}: {count} message | thiếu event_timestamp_utc: {missing_event_counts[topic]} "
          f"| từ {start} đến {end} (giờ VN)")
    print("Theo mã:", dict(symbol_counts[topic]))

print()
for name, values in delays.items():
    if values:
        values.sort()
        print(f"Độ trễ {name}, giây: trung vị {statistics.median(values):.3f}, "
              f"p95 {values[int(len(values) * 0.95)]:.3f}, nhỏ nhất {values[0]:.3f}, lớn nhất {values[-1]:.3f}")