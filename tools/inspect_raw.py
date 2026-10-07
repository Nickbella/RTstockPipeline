import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VN = ZoneInfo("Asia/Ho_Chi_Minh")
topics, null_ts, symbols = Counter(), Counter(), defaultdict(Counter)
first, last, lat, bad = {}, {}, [], 0

for f in sorted(Path("data/raw/dnse").rglob("*.jsonl")):
    with f.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                bad += 1
                continue
            t = r.get("topic")
            topics[t] += 1
            symbols[t][r.get("symbol")] += 1
            rt = r.get("recv_ts")
            first.setdefault(t, rt)
            last[t] = rt
            if r.get("event_ts") is None:
                null_ts[t] += 1
            elif t == "trade":
                d = (datetime.fromisoformat(rt) - datetime.fromisoformat(r["event_ts"])).total_seconds()
                lat.append(d)

print("Dòng hỏng:", bad)
for t, n in topics.most_common():
    a = datetime.fromisoformat(first[t]).astimezone(VN).strftime("%H:%M:%S")
    b = datetime.fromisoformat(last[t]).astimezone(VN).strftime("%H:%M:%S")
    print(f"\n=== {t}: {n} message | event_ts null: {null_ts[t]} | từ {a} đến {b} (giờ VN)")
    print("Theo mã:", dict(symbols[t]))

if lat:
    lat.sort()
    print(f"\nĐộ trễ trade (recv_ts - event_ts), giây: trung vị {statistics.median(lat):.2f}, "
          f"p95 {lat[int(len(lat)*0.95)]:.2f}, nhỏ nhất {lat[0]:.2f}, lớn nhất {lat[-1]:.2f}")