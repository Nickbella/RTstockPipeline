"""Kafka: clean.trade -> khử trùng -> build_candles -> clean.candle_1m (upsert theo key)."""
import json, logging, os, time
from datetime import timedelta
from confluent_kafka import Consumer, Producer
from consumer.candle_builder import build_candles, floor_time
from consumer.normalizer import to_trade

log = logging.getLogger("dnse.candles")
logging.basicConfig(level=logging.INFO)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REPLAY_MINUTES = int(os.getenv("REPLAY_MINUTES", "10"))   # 0 = đọc lại từ đầu topic
KEEP = timedelta(minutes=3)                                # cửa sổ trade giữ trong bộ nhớ
FLUSH_EVERY, FLUSH_SECONDS = 2000, 5

trades, seen, published = {}, set(), {}     # trades: khóa khử trùng -> Trade
watermark = None


def add(row):
    """Khử trùng theo khóa nghiệp vụ (chịu được cả SDK gửi lại lẫn xử lý lại của normalizer)."""
    global watermark
    key = (row["ma_ck"], row["thoi_gian_vn"], row["gia"], row["khoi_luong"], row.get("khoi_luong_luy_ke"))
    trade = to_trade(row)
    if trade is None or key in trades:
        return
    if watermark and floor_time(trade.time, 60) < floor_time(watermark, 60) - KEEP:
        return                                              # quá muộn: bucket đã bị dọn, ghi đè sẽ sai
    trades[key] = trade
    if watermark is None or trade.time > watermark:
        watermark = trade.time


def flush(producer):
    """Dựng lại nến từ cửa sổ, chỉ phát nến đã đổi, rồi dọn trade cũ."""
    global trades
    for c in build_candles(trades.values()):
        key = f"{c.symbol}|{c.bucket.isoformat()}|{c.session}"
        value = (c.volume, c.value, c.trades, c.open, c.close, c.high, c.low)
        if published.get(key) != value:
            published[key] = value
            body = {**c._asdict(), "bucket": c.bucket.isoformat(),
                    "ngay_giao_dich": c.bucket.date().isoformat()}
            producer.produce("clean-candle-1m", key=key.encode(), value=json.dumps(body).encode())
    if watermark:
        cutoff = floor_time(watermark, 60) - KEEP
        trades = {k: t for k, t in trades.items() if floor_time(t.time, 60) >= cutoff}
        for k in [k for k in published if k.split("|")[1] < cutoff.isoformat()[:16]]:
            published.pop(k, None)                          # so sánh chuỗi ISO cùng múi giờ +07:00
    producer.flush()


def on_assign(consumer, partitions):
    """Không dùng offset đã commit: trạng thái nằm trong bộ nhớ nên phải đọc lại vài phút gần nhất."""
    if REPLAY_MINUTES > 0:
        start = int((time.time() - REPLAY_MINUTES * 60) * 1000)
        for p in partitions:
            p.offset = start                                # đây là timestamp (ms)
        partitions[:] = consumer.offsets_for_times(partitions)
    else:
        for p in partitions:
            p.offset = 0
    consumer.assign(partitions)


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dnse-candles",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "linger.ms": 50})
    consumer.subscribe(["clean-trade"], on_assign=on_assign)
    count, last = 0, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    add(json.loads(msg.value()))
                except Exception:
                    log.exception("Bỏ qua bản ghi lỗi")
                count += 1
            if count >= FLUSH_EVERY or (count and time.monotonic() - last > FLUSH_SECONDS):
                flush(producer)
                count, last = 0, time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        flush(producer)
        consumer.close()


if __name__ == "__main__":
    main()