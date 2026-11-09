from consumer.normalizer import FIELDS

RAW_TOPICS = ("trade", "ohlc_1_minute", "quote", "foreign_trading", "expected_price")
COMMON = {"kafka_topic": "text", "kafka_partition": "int", "kafka_offset": "bigint",
          "symbol": "text NOT NULL", "time_vn": "timestamp NOT NULL", "trading_date": "date",
          "time_source": "text", "received_time_vn": "timestamp", "sdk_time_vn": "timestamp",
          "sequence_number": "bigint"}
PK = "PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)"


def schema(topic):
    extra = {n: t for n, (_, _, t) in FIELDS[topic].items()} if topic in FIELDS else {"data": "jsonb"}
    return {**COMMON, **extra}


def create_tables(conn):
    with conn.transaction():
        for topic in RAW_TOPICS:
            cols = ", ".join(f"{n} {t}" for n, t in schema(topic).items())
            conn.execute(f"CREATE TABLE IF NOT EXISTS {topic} ({cols}, {PK})")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{topic}_symbol_time ON {topic} (symbol, time_vn)")
        conn.execute(f"CREATE TABLE IF NOT EXISTS error_records (kafka_topic text, kafka_partition int, "
                     f"kafka_offset bigint, error text, content text, {PK})")