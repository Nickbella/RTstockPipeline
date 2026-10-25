"""Kafka: dnse.<topic> (thô) -> normalize() -> clean.<topic> (sạch); lỗi -> dnse.dead_letter."""
import json, logging, os, time
from confluent_kafka import Consumer, Producer
from consumer.normalizer import normalize

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("dnse.normalizer")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPICS = [f"dnse.{t}" for t in ("trade", "ohlc_1_minute", "quote", "foreign_trading", "expected_price")]
COMMIT_EVERY, COMMIT_SECONDS = 500, 2


def handle(msg, producer):
    try:
        row = normalize(json.loads(msg.value()))
        if row is None:
            raise ValueError("bản ghi không dùng được")
        row["kafka"] = f"{msg.partition()}:{msg.offset()}"      
        producer.produce(f"clean.{row['topic']}", key=row["ma_ck"].encode(),
                         value=json.dumps(row, ensure_ascii=False).encode())
    except Exception as error:
        producer.produce("dnse.dead_letter", key=msg.key(), value=msg.value(),
                         headers={"loi": str(error), "nguon": msg.topic()})


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dnse-normalizer",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                         "linger.ms": 50, "compression.type": "lz4"})
    consumer.subscribe(TOPICS)
    pending, last_commit = 0, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error(msg.error())
                else:
                    handle(msg, producer)
                    pending += 1
                producer.poll(0)
            if pending and (pending >= COMMIT_EVERY or time.monotonic() - last_commit > COMMIT_SECONDS):
                producer.flush()                                  # đầu ra đã an toàn rồi mới commit offset đầu vào
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