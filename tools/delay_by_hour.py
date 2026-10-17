"""Độ trễ của trade (collector_received - event) theo giờ và theo phút, để xem đuôi trễ dồn vào đâu.

Chạy từ thư mục gốc: python -m tools.delay_by_hour              (hôm nay, giờ VN)
                      python -m tools.delay_by_hour 2026-10-16   (ngày cụ thể)
"""
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
MINIMUM_MESSAGES_PER_MINUTE = 20      # bỏ qua phút quá ít message để số đo không bị nhiễu

selected_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(VIETNAM_TIMEZONE).strftime("%Y-%m-%d")
file_paths = sorted((Path("data/raw/dnse") / selected_date).glob("*.jsonl"))
print(f"Ngày: {selected_date} | đọc {len(file_paths)} tệp")

delays_by_hour, delays_by_minute = defaultdict(list), defaultdict(list)
for file_path in file_paths:
    with file_path.open(encoding="utf-8") as file_handle:
        for line in file_handle:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            event_text = record.get("event_timestamp_utc")
            received_text = record.get("collector_received_timestamp_utc")
            if record.get("topic") != "trade" or not event_text or not received_text:
                continue
            received_time = datetime.fromisoformat(received_text)
            delay_seconds = (received_time - datetime.fromisoformat(event_text)).total_seconds()
            received_vietnam_time = received_time.astimezone(VIETNAM_TIMEZONE)
            delays_by_hour[received_vietnam_time.strftime("%H")].append(delay_seconds)
            delays_by_minute[received_vietnam_time.strftime("%H:%M")].append(delay_seconds)


def describe(delays):
    sorted_delays = sorted(delays)
    return (f"n={len(sorted_delays):>6} | trung vị {statistics.median(sorted_delays):7.3f}s | "
            f"p95 {sorted_delays[int(len(sorted_delays) * 0.95)]:7.3f}s | lớn nhất {sorted_delays[-1]:7.3f}s")


print("\nTheo giờ (giờ VN):")
for hour, delays in sorted(delays_by_hour.items()):
    print(f"{hour}h    {describe(delays)}")

busy_minutes = [(minute, delays) for minute, delays in delays_by_minute.items()
                if len(delays) >= MINIMUM_MESSAGES_PER_MINUTE]
print(f"\n5 phút có độ trễ trung vị lớn nhất (chỉ tính phút có từ {MINIMUM_MESSAGES_PER_MINUTE} message):")
for minute, delays in sorted(busy_minutes, key=lambda item: statistics.median(item[1]), reverse=True)[:5]:
    print(f"{minute}  {describe(delays)}")