from consumer.normalizer import FIELDS

RAW_TOPICS = ("trade", "ohlc_1_minute", "quote", "foreign_trading", "expected_price")
COMMON = {"kafka_topic": "text", "kafka_partition": "int", "kafka_offset": "bigint", "ma_ck": "text NOT NULL",
          "thoi_gian_vn": "timestamptz NOT NULL", "ngay_giao_dich": "date", "nguon_thoi_gian": "text",
          "thoi_gian_nhan_vn": "timestamptz", "thoi_gian_sdk_vn": "timestamptz", "so_thu_tu": "bigint"}
PK = "PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)"


def schema(topic):
    extra = {n: t for n, (_, _, t) in FIELDS[topic].items()} if topic in FIELDS else {"du_lieu": "jsonb"}
    return {**COMMON, **extra}


def create_tables(conn):
    with conn.transaction():
        for topic in RAW_TOPICS:
            cols = ", ".join(f"{n} {t}" for n, t in schema(topic).items())
            conn.execute(f"CREATE TABLE IF NOT EXISTS {topic} ({cols}, {PK})")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{topic}_ma_ck_gio ON {topic} (ma_ck, thoi_gian_vn)")
        conn.execute(f"CREATE TABLE IF NOT EXISTS ban_ghi_loi (kafka_topic text, kafka_partition int, "
                     f"kafka_offset bigint, loi text, noi_dung text, {PK})")