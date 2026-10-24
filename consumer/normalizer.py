"""Chuẩn hóa bản ghi thô (JSONL/Kafka) -> dict sạch: tên tiếng Việt, giờ UTC+7, bỏ null."""
from datetime import datetime
from zoneinfo import ZoneInfo
from consumer.candle_builder import Trade


VN = ZoneInfo("Asia/Ho_Chi_Minh")
KEYS = {"nguon": ("Nguồn dữ liệu", "source"), "topic": ("Chủ đề", "topic"),
        "ma": ("Mã CK", "symbol"), "payload": ("Nội dung gốc", "payload"),
        "stt": ("Số thứ tự", "sequence_number", "seq"),
        "san": ("Thời gian gốc (UTC)", "event_timestamp_utc", "event_ts"),
        "nhan": ("Thời gian collector ghi nhận (UTC)", "collector_received_timestamp_utc", "recv_ts"),
        "sdk": ("Thời gian SDK ghi nhận (UTC)", "sdk_received_timestamp_utc", "sdk_recv_ts")}

# topic -> {tên_VN: (tên trường payload, kiểu)}. Giá đơn vị NGHÌN ĐỒNG (20.5 = 20.500đ).
FIELDS = {
    "trade": {"gia": ("price", float), "khoi_luong": ("quantity", lambda x: int(round(float(x)))),
              "khoi_luong_luy_ke": ("totalVolumeTraded", int), "ma_phien": ("tradingSessionId", str),
              "san_giao_dich": ("marketId", str)},
    "ohlc_1_minute": {"mo": ("open", float), "cao": ("high", float), "thap": ("low", float),   # ĐOÁN, cần mẫu thật
                      "dong": ("close", float), "khoi_luong": ("volume", int)},
}
REQUIRED = {"trade": ("gia", "khoi_luong"), "ohlc_1_minute": ("mo", "cao", "thap", "dong")}


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


def to_vn(iso_text):
    return datetime.fromisoformat(iso_text).astimezone(VN) if iso_text else None


def normalize(record):
    """Trả về dict sạch, hoặc None nếu bản ghi không dùng được."""
    payload = clean(pick(record, "payload") or {})
    topic = pick(record, "topic")
    symbol = str(pick(record, "ma") or payload.get("symbol") or "").upper()
    event_time, receive_time = to_vn(pick(record, "san")), to_vn(pick(record, "nhan"))
    time_vn = event_time or receive_time        # topic không có giờ sàn -> dùng giờ collector
    if not symbol or not time_vn:
        return None

    row = {"nguon": pick(record, "nguon"), "topic": topic, "ma_ck": symbol,
           "thoi_gian_vn": time_vn.isoformat(), "ngay_giao_dich": time_vn.date().isoformat(),
           "nguon_thoi_gian": "san" if event_time else "collector",
           "thoi_gian_nhan_vn": receive_time.isoformat() if receive_time else None,
           "thoi_gian_sdk_vn": (to_vn(pick(record, "sdk")) or time_vn).isoformat() if pick(record, "sdk") else None,
           "so_thu_tu": pick(record, "stt")}

    mapping = FIELDS.get(topic)
    if mapping is None:                         # topic chưa định nghĩa: giữ payload đã làm sạch
        row["du_lieu"] = payload
        return clean(row)
    for vn_name, (field, cast) in mapping.items():
        try:
            row[vn_name] = cast(payload[field]) if field in payload else None
        except (TypeError, ValueError):
            row[vn_name] = None
    if any(row.get(f) is None for f in REQUIRED.get(topic, ())):
        return None
    if topic == "trade" and (row["gia"] <= 0 or row["khoi_luong"] <= 0):
        return None
    return clean(row)


def to_trade(row):
    """Dòng trade sạch -> Trade cho candle_builder (chỉ khi có giờ sàn)."""
    if row.get("nguon_thoi_gian") != "san":
        return None
    return Trade(row["ma_ck"], datetime.fromisoformat(row["thoi_gian_vn"]), row["gia"], row["khoi_luong"],
                 (row.get("thoi_gian_nhan_vn"), row.get("so_thu_tu")))