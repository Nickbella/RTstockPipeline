"""Đưa bản ghi thô định dạng cũ về định dạng hiện tại (dùng cho replay và bước tiền xử lý)."""

LEGACY_TOPIC_NAMES = {"ohlc_1": "ohlc_1_minute", "foreign": "foreign_trading"}
LEGACY_KEY_NAMES = {
    "event_ts": "event_timestamp_utc",
    "recv_ts": "collector_received_timestamp_utc",
    "sdk_recv_ts": "sdk_received_timestamp_utc",
    "seq": "sequence_number",
}


def normalize_record(record):
    """Đổi khóa và tên topic cũ sang tên đầy đủ. Bản ghi định dạng mới đi qua không đổi."""
    normalized_record = {LEGACY_KEY_NAMES.get(key, key): value for key, value in record.items()}
    topic = normalized_record.get("topic")
    normalized_record["topic"] = LEGACY_TOPIC_NAMES.get(topic, topic)
    return normalized_record