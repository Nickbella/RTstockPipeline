import json
import logging

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger("dnse.kafka")


def to_kafka_topic_name(topic, topic_prefix="dnse-"):
    """trade -> dnse-trade, ohlc_1_minute -> dnse-ohlc-1-minute. Dữ liệu replay dùng tiền tố khác (replay-dnse-)."""
    return topic_prefix + topic.replace("_", "-")


def create_missing_topics(bootstrap_servers, topics, partition_count=3, topic_prefix="dnse-"):
    """Tạo các topic Kafka còn thiếu (replication factor 1 vì chỉ có một broker). Trả về tên các topic đã tạo."""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing_topic_names = admin_client.list_topics(timeout=10).topics
    new_topics = [NewTopic(name, num_partitions=partition_count, replication_factor=1)
                  for name in (to_kafka_topic_name(topic, topic_prefix) for topic in topics)
                  if name not in existing_topic_names]
    if new_topics:
        for future in admin_client.create_topics(new_topics).values():
            future.result()
    return [new_topic.topic for new_topic in new_topics]


class KafkaWriter:
    """Gửi mỗi bản ghi lên Kafka: topic theo loại dữ liệu, khóa = mã chứng khoán, giá trị = JSON của bản ghi.

    Cùng giao diện với RawWriter (write_batch, close) nên gắn được vào QueuedSink.
    """

    def __init__(self, bootstrap_servers, topic_prefix="dnse-"):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 20,                              # gom message trong 20 ms để gửi theo lô
            "compression.type": "lz4",
            "queue.buffering.max.messages": 200_000,      # giới hạn hàng đợi nội bộ của thư viện
        })
        self.topic_prefix = topic_prefix
        self.delivered_message_count = 0
        self.failed_message_count = 0

    def write_batch(self, keyed_records):
        for _file_key, record in keyed_records:
            topic = record.get("Chủ đề") or record.get("topic")
            symbol = record.get("Mã CK") or record.get("symbol")
            try:
                self.producer.produce(
                    to_kafka_topic_name(topic, self.topic_prefix),
                    value=json.dumps(record, ensure_ascii=False, default=str).encode("utf-8"),
                    key=symbol.encode("utf-8") if symbol else None,
                    on_delivery=self._on_delivery)
            except BufferError:
                self.failed_message_count += 1
            self.producer.poll(0)                             # xử lý các callback giao hàng đang chờ

    def close(self, flush_timeout_seconds=10):
        remaining_message_count = self.producer.flush(flush_timeout_seconds)
        if remaining_message_count:
            logger.error("Còn %d message chưa gửi được lên Kafka khi đóng", remaining_message_count)

    def _on_delivery(self, error, message):
        if error is None:
            self.delivered_message_count += 1
        else:
            self.failed_message_count += 1
            if self.failed_message_count == 1:
                logger.error("Gửi Kafka thất bại: %s (các lỗi sau chỉ được đếm)", error)