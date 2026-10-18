import json
import logging

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger("dnse.kafka")


def to_kafka_topic_name(topic, topic_prefix="dnse-"):
    """trade -> dnse-trade, ohlc_1_minute -> dnse-ohlc-1-minute (chỉ dùng dấu gạch ngang).

    Dữ liệu phát lại (replay) dùng tiền tố khác, ví dụ replay-dnse-, để không lẫn với dữ liệu thật.
    """
    return topic_prefix + topic.replace("_", "-")


def create_missing_topics(bootstrap_servers, topics, partition_count=3, timeout_seconds=10,
                          topic_prefix="dnse-"):
    """Tạo các topic Kafka còn thiếu (replication factor 1 vì chỉ có một broker). Trả về danh sách tên đã tạo."""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing_topic_names = admin_client.list_topics(timeout=timeout_seconds).topics
    new_topics = [NewTopic(name, num_partitions=partition_count, replication_factor=1)
                  for name in (to_kafka_topic_name(topic, topic_prefix) for topic in topics)
                  if name not in existing_topic_names]
    if not new_topics:
        return []
    for topic_name, future in admin_client.create_topics(new_topics).items():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return [new_topic.topic for new_topic in new_topics]


class KafkaWriter:
    """Gửi mỗi bản ghi lên Kafka: topic theo loại dữ liệu, khóa = mã chứng khoán, giá trị = JSON của bản ghi.

    Cùng giao diện với RawWriter (write_batch, close) nên gắn được vào QueuedSink.
    Producer của confluent-kafka gửi bất đồng bộ; kết quả giao hàng được đếm trong _on_delivery.
    """

    def __init__(self, bootstrap_servers, extra_config=None, topic_prefix="dnse-"):
        config = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 20,                              # gom message trong 20 ms để gửi theo lô
            "compression.type": "lz4",
            "queue.buffering.max.messages": 200_000,      # giới hạn hàng đợi nội bộ của thư viện
        }
        config.update(extra_config or {})
        self.producer = Producer(config)
        self.topic_prefix = topic_prefix
        self.delivered_message_count = 0
        self.failed_message_count = 0

    def write_batch(self, keyed_records):
        for _file_key, record in keyed_records:
            try:
                value = json.dumps(record, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                fallback_record = dict(record, payload=repr(record.get("payload")), payload_encoding_error=True)
                value = json.dumps(fallback_record, ensure_ascii=False, default=str)
            self._produce(to_kafka_topic_name(record["topic"], self.topic_prefix), record.get("symbol"), value)
        self.producer.poll(0)                              # xử lý các callback giao hàng đang chờ

    def close(self, flush_timeout_seconds=10):
        remaining_message_count = self.producer.flush(flush_timeout_seconds)
        if remaining_message_count:
            logger.error("Còn %d message chưa gửi được lên Kafka khi đóng", remaining_message_count)

    def _produce(self, kafka_topic, symbol, value):
        for attempt_number in range(2):
            try:
                self.producer.produce(kafka_topic, value=value.encode("utf-8"),
                                      key=symbol.encode("utf-8") if symbol else None,
                                      on_delivery=self._on_delivery)
                return
            except BufferError:
                self.producer.poll(0.1)                    # hàng đợi nội bộ đầy: chờ gửi bớt rồi thử lại
        self.failed_message_count += 1

    def _on_delivery(self, error, message):
        if error is None:
            self.delivered_message_count += 1
        else:
            self.failed_message_count += 1
            if self.failed_message_count == 1:
                logger.error("Gửi Kafka thất bại: %s (các lỗi sau chỉ được đếm)", error)