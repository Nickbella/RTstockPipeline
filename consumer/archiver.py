"""Cầu nối: Kafka dnse-<topic> (thô) -> normalize() -> PostgreSQL (bảng cùng tên topic; lỗi -> ban_ghi_loi)."""
import json, logging, os, time
import psycopg
from psycopg.types.json import Jsonb
from confluent_kafka import Consumer
from consumer.normalizer import FIELDS, normalize
from utils.kafka_writer import to_kafka_topic_name
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("dnse.archiver")
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


def save(conn, rows, errors):
    """Một transaction cho cả lô; ON CONFLICT làm cho việc ghi lại (sau sự cố) không sinh dòng trùng."""
    by_topic = {}
    for row in rows:
        if "du_lieu" in row:
            row["du_lieu"] = Jsonb(row["du_lieu"])
        by_topic.setdefault(row["topic"], []).append(row)
    with conn.transaction(), conn.cursor() as cur:
        for topic, group in by_topic.items():
            cols = list(schema(topic))
            cur.executemany(f"INSERT INTO {topic} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))}) "
                            f"ON CONFLICT DO NOTHING", [[r.get(c) for c in cols] for r in group])
        cur.executemany("INSERT INTO ban_ghi_loi VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", errors)


def main():
    logging.basicConfig(level=logging.INFO)
    dsn = os.environ["DATABASE_URL"]                      # postgresql://user:pass@localhost:5432/dnse

    def connect():
        while True:
            try:
                return psycopg.connect(dsn)
            except psycopg.OperationalError as error:
                log.error("Chưa kết nối được DB: %s. Thử lại sau 5s", error)
                time.sleep(5)

    conn, total = connect(), 0
    create_tables(conn)
    consumer = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                         "group.id": "dnse-db-writer", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([to_kafka_topic_name(t) for t in RAW_TOPICS])
    try:
        while True:
            rows, errors = [], []
            for msg in consumer.consume(500, 1.0):        # tối đa 500 message hoặc chờ 1 giây
                if msg.error():
                    log.error(msg.error())
                    continue
                try:
                    row = normalize(json.loads(msg.value()))
                    if row is None or row["topic"] not in RAW_TOPICS:
                        raise ValueError("bản ghi không dùng được")
                    row.update(kafka_topic=msg.topic(), kafka_partition=msg.partition(), kafka_offset=msg.offset())
                    rows.append(row)
                except Exception as error:
                    errors.append((msg.topic(), msg.partition(), msg.offset(), str(error),
                                   (msg.value() or b"").decode("utf-8", "replace")))
            if not (rows or errors):
                continue
            while True:                                   # lỗi kết nối DB: thử lại, KHÔNG commit offset
                try:
                    save(conn, rows, errors)
                    break
                except psycopg.OperationalError as error:
                    log.error("DB lỗi: %s", error)
                    time.sleep(5)
                    conn = connect()
            consumer.commit(asynchronous=False)           # chỉ commit offset sau khi DB đã commit
            total += len(rows)
            log.info("Ghi %d dòng sạch, %d lỗi (tổng sạch %d)", len(rows), len(errors), total)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()