"""Chuẩn hóa dữ liệu thô: Kafka dnse-<topic> -> clean-<topic> (tên VN, giờ UTC+7, bỏ null); lỗi -> dnse-dead-letter."""
import json, logging, os, time
from datetime import datetime
from zoneinfo import ZoneInfo

VN = ZoneInfo("Asia/Ho_Chi_Minh")
log = logging.getLogger("dnse.normalizer")
RAW_TOPICS = ("trade", "ohlc_1_minute", "quote", "foreign_trading", "expected_price")
KEYS = {"nguon": ("Nguồn dữ liệu", "source"), "topic": ("Chủ đề", "topic"), "ma": ("Mã CK", "symbol"),
        "payload": ("Nội dung gốc", "payload"), "stt": ("Số thứ tự", "sequence_number", "seq"),
        "san": ("Thời gian gốc (UTC)", "event_timestamp_utc", "event_ts"),
        "nhan": ("Thời gian collector ghi nhận (UTC)", "collector_received_timestamp_utc", "recv_ts"),
        "sdk": ("Thời gian SDK ghi nhận (UTC)", "sdk_received_timestamp_utc", "sdk_recv_ts")}
FIELDS = {
    "trade": {"gia": ("price", float), "khoi_luong": ("quantity", lambda x: int(round(float(x)))),
              "khoi_luong_luy_ke": ("totalVolumeTraded", int), "ma_phien": ("tradingSessionId", str),
              "san_giao_dich": ("marketId", str)},
    "ohlc_1_minute": {"mo": ("open", float), "cao": ("high", float), "thap": ("low", float),
                      "dong": ("close", float), "khoi_luong": ("volume", int), "cap_nhat": ("lastUpdated", int),
                      "loai": ("type", str), "do_phan_giai": ("resolution", str)},
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
    row = {"nguon": pick(record, "nguon"), "topic": topic, "ma_ck": symbol,
           "thoi_gian_vn": time_vn.isoformat(), "ngay_giao_dich": time_vn.date().isoformat(),
           "nguon_thoi_gian": "san" if event_time else "collector",
           "thoi_gian_nhan_vn": receive_time.isoformat() if receive_time else None,
           "thoi_gian_sdk_vn": sdk_time.isoformat() if sdk_time else None, "so_thu_tu": pick(record, "stt")}
    mapping = FIELDS.get(topic)
    if mapping is None:                                   # topic chưa có mẫu: giữ payload đã làm sạch
        row["du_lieu"] = payload
        return clean(row)
    for name, (field, cast) in mapping.items():
        try:
            row[name] = cast(payload[field]) if field in payload else None
        except (TypeError, ValueError):
            row[name] = None
    if any(row.get(f) is None for f in REQUIRED.get(topic, ())):
        return None
    if topic == "trade" and (row["gia"] <= 0 or row["khoi_luong"] <= 0):
        return None
    return clean(row)


def to_trade(row):
    """Dòng trade sạch -> Trade cho candle_builder (chỉ khi có giờ sàn)."""
    from consumer.candle_builder import Trade
    if row.get("nguon_thoi_gian") != "san":
        return None
    return Trade(row["ma_ck"], datetime.fromisoformat(row["thoi_gian_vn"]), row["gia"], row["khoi_luong"],
                 (row.get("thoi_gian_nhan_vn"), row.get("so_thu_tu")))


def main():
    from confluent_kafka import Consumer, Producer       # nhập muộn: test không cần cài Kafka
    from utils.kafka_writer import to_kafka_topic_name
    logging.basicConfig(level=logging.INFO)
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = Consumer({"bootstrap.servers": servers, "group.id": "dnse-normalizer",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": servers, "enable.idempotence": True,
                         "linger.ms": 50, "compression.type": "lz4"})
    consumer.subscribe([to_kafka_topic_name(t) for t in RAW_TOPICS])
    pending, last_commit = 0, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error(msg.error())
                else:
                    try:
                        row = normalize(json.loads(msg.value()))
                        if row is None:
                            raise ValueError("bản ghi không dùng được")
                        row["kafka"] = f"{msg.partition()}:{msg.offset()}"
                        producer.produce("clean-" + row["topic"].replace("_", "-"), key=row["ma_ck"].encode(),
                                         value=json.dumps(row, ensure_ascii=False).encode())
                    except Exception as error:
                        producer.produce("dnse-dead-letter", key=msg.key(), value=msg.value(),
                                         headers={"loi": str(error), "nguon": msg.topic()})
                    pending += 1
                producer.poll(0)
            if pending and (pending >= 500 or time.monotonic() - last_commit > 2):
                producer.flush()                          # đầu ra an toàn rồi mới commit offset đầu vào
                consumer.commit(asynchronous=False)
                pending, last_commit = 0, time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        if pending:
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()