"""In một payload mẫu cho mỗi topic của một ngày.

Chạy từ thư mục gốc: python -m tools.sample_payloads              (hôm nay, giờ VN)
                      python -m tools.sample_payloads 2026-10-16   (ngày cụ thể)
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
selected_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(VIETNAM_TIMEZONE).strftime("%Y-%m-%d")
file_paths = sorted((Path("data/raw/dnse") / selected_date).glob("*.jsonl"))
print(f"Ngày: {selected_date} | đọc {len(file_paths)} tệp")

sample_by_topic = {}
for file_path in file_paths:
    with file_path.open(encoding="utf-8") as file_handle:
        for line in file_handle:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            sample_by_topic.setdefault(record.get("topic"), record.get("payload"))

for topic, payload in sample_by_topic.items():
    print(f"\n=== {topic}")
    print(json.dumps(payload, ensure_ascii=False))