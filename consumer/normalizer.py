"""Chuẩn hóa bản ghi thô (khóa Việt/Anh/cũ) -> dict sạch: tên VN, giờ UTC+7, bỏ null. Không dính tới Kafka hay DB."""
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

VN = ZoneInfo("Asia/Ho_Chi_Minh")
CONFIG = json.loads((Path(__file__).resolve().parent.parent / "config" / "schema.json").read_text(encoding="utf-8"))
KEYS, REQUIRED = CONFIG["keys"], CONFIG["required"]
CASTS = {"float": float, "int": lambda x: int(round(float(x))), "str": str}
FIELDS = {topic: {name: (field, CASTS[cast], sql) for name, (field, cast, sql) in cols.items()}
          for topic, cols in CONFIG["fields"].items()}


def pick(record, name):
    return next((record[k] for k in KEYS[name] if record.get(k) is not None), None)


def clean(value):
    """Bỏ None, chuỗi rỗng, NaN; đệ quy qua dict/list."""
    if isinstance(value, dict):
        return {k: v for k, x in value.items() if (v := clean(x)) is not None}
    if isinstance(value, list):
        return [v for x in value if (v := clean(x)) is not None]
    if value is None or value == "" or value != value:
        return None
    return value.strip() if isinstance(value, str) else value


def to_vn(text):
    return datetime.fromisoformat(text).astimezone(VN) if text else None


def normalize(record):
    """Bản ghi thô -> dict sạch, hoặc None nếu không dùng được."""
    payload = clean(pick(record, "payload") or {})
    topic = pick(record, "topic")
    symbol = str(pick(record, "ma") or payload.get("symbol") or "").upper()
    event_time, receive_time, sdk_time = (to_vn(pick(record, k)) for k in ("san", "nhan", "sdk"))
    time_vn = event_time or receive_time
    if not symbol or not time_vn:
        return None
    row = {"topic": topic, "ma_ck": symbol, "thoi_gian_vn": time_vn.isoformat(),
           "ngay_giao_dich": time_vn.date().isoformat(), "nguon_thoi_gian": "san" if event_time else "collector",
           "thoi_gian_nhan_vn": receive_time.isoformat() if receive_time else None,
           "thoi_gian_sdk_vn": sdk_time.isoformat() if sdk_time else None, "so_thu_tu": pick(record, "stt")}
    if topic not in FIELDS:                               # chưa có mẫu: giữ payload đã làm sạch
        row["du_lieu"] = payload
        return clean(row)
    for name, (field, cast, _) in FIELDS[topic].items():
        try:
            row[name] = cast(payload[field]) if field in payload else None
        except (TypeError, ValueError):
            row[name] = None
    if any(row.get(f) is None for f in REQUIRED[topic]):
        return None
    if topic == "trade" and (row["gia"] <= 0 or row["khoi_luong"] <= 0):
        return None
    return clean(row)