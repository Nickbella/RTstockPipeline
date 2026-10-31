"""Cầu nối: Kafka dnse-<topic> (thô) -> normalize() -> rdbms (PostgreSQL)."""
import json, logging, os
import psycopg
from confluent_kafka import Consumer
from consumer.normalizer import normalize
from rdbms import RAW_TOPICS, connect, create_tables, save
from utils.kafka_writer import to_kafka_topic_name

log = logging.getLogger("dnse.archiver")


def main():
    logging.basicConfig(level=logging.INFO)
    conn, total = connect(), 0
    create_tables(conn)
    consumer = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                         "group.id": "dnse-db-writer", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([to_kafka_topic_name(t) for t in RAW_TOPICS])
    try:
        while True:
            rows, errors = [], []
            for msg in consumer.consume(500, 1.0):
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
            while True:
                try:
                    save(conn, rows, errors)
                    break
                except psycopg.OperationalError as error:
                    log.error("DB lỗi: %s", error)
                    conn = connect()
            consumer.commit(asynchronous=False)
            total += len(rows)
            log.info("Ghi %d dòng sạch, %d lỗi (tổng sạch %d)", len(rows), len(errors), total)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()