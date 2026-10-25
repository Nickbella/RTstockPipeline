"""Kafka: clean-trade + clean-ohlc-1-minute -> nến tự dựng (clean-candle-1m) + đối soát với nến sàn (clean-ohlc-diff)."""
import json, logging, os, time
from collections import defaultdict
from datetime import timedelta
from confluent_kafka import Consumer, Producer
from consumer.candle_builder import build_candles, floor_time
from consumer.normalizer import to_trade

log = logging.getLogger("dnse.candles")
logging.basicConfig(level=logging.INFO)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REPLAY_MINUTES = int(os.getenv("REPLAY_MINUTES", "10"))      # 0 = đọc lại từ đầu topic
OHLC = "clean-ohlc-1-minute"
KEEP = timedelta(minutes=3)

trades, published, official, checked = {}, {}, {}, set()    # official: (ma_ck, giờ) -> nến sàn mới nhất
stats = {"khop": 0, "lech": 0, "bo_trade": 0}
watermark = None


def add(row):
    """Trade: khử trùng theo khóa nghiệp vụ, bỏ trade thiếu giờ sàn hoặc đến quá muộn."""
    global watermark
    trade, key = to_trade(row), (row["ma_ck"], row["thoi_gian_vn"], row["gia"], row["khoi_luong"],
                                 row.get("khoi_luong_luy_ke"))
    if trade is None or (watermark and floor_time(trade.time, 60) < floor_time(watermark, 60) - KEEP):
        stats["bo_trade"] += 1
    elif key not in trades:
        trades[key] = trade
        watermark = max(watermark or trade.time, trade.time)


def add_ohlc(row):
    """Nến sàn: giữ bản có cap_nhat lớn nhất; bỏ chỉ số (VN30) vì không có trade để đối soát."""
    key = (row["ma_ck"], row["thoi_gian_vn"])
    if row.get("loai") == "STOCK" and row.get("cap_nhat", 0) >= official.get(key, {}).get("cap_nhat", 0):
        official[key] = row


def reconcile(producer, candles):
    """So 5 trường với nến tự dựng, chỉ với phút đã đóng chắc chắn (trước watermark ≥ 1 phút)."""
    mine, limit = defaultdict(list), (floor_time(watermark, 60) - timedelta(minutes=1)).isoformat()
    for c in candles:
        mine[(c.symbol, c.bucket.isoformat())].append(c)
    for key, o in official.items():
        g = mine.get(key)
        if key[1] > limit or key in checked or not g:
            continue
        checked.add(key)
        own = {"khoi_luong": sum(c.volume for c in g), "cao": max(c.high for c in g),
               "thap": min(c.low for c in g), "mo": g[0].open, "dong": g[-1].close}
        diff = {n: {"tu_dung": v, "san": o[n]} for n, v in own.items() if o.get(n) is not None and abs(v - o[n]) > 1e-6}
        stats["lech" if diff else "khop"] += 1
        if diff:
            log.warning("Lệch nến %s %s: %s", key[0], key[1], diff)
            producer.produce("clean-ohlc-diff", key=key[0].encode(), value=json.dumps(
                {"ma_ck": key[0], "thoi_gian_vn": key[1], "lech": diff}, ensure_ascii=False).encode())


def flush(producer):
    """Dựng lại nến từ cửa sổ, phát nến đã đổi, đối soát, dọn dữ liệu cũ."""
    global trades, published, official
    candles = build_candles(trades.values())
    for c in candles:
        key, value = (c.symbol, c.bucket.isoformat(), c.session), (c.volume, c.value, c.trades, c.open, c.close, c.high, c.low)
        if published.get(key) != value:
            published[key] = value
            producer.produce("clean-candle-1m", key="|".join(key).encode(), value=json.dumps(
                {**c._asdict(), "bucket": key[1], "ngay_giao_dich": c.bucket.date().isoformat()}, default=str).encode())
    if watermark:
        reconcile(producer, candles)
        cutoff = floor_time(watermark, 60) - KEEP
        text = cutoff.isoformat()                            # so sánh chuỗi ISO cùng múi giờ +07:00
        trades = {k: t for k, t in trades.items() if floor_time(t.time, 60) >= cutoff}
        published = {k: v for k, v in published.items() if k[1] >= text}
        official = {k: v for k, v in official.items() if k[1] >= text}
        checked.intersection_update(official)
    producer.flush()
    log.info("Đối soát %s | trade trong cửa sổ %d", stats, len(trades))


def on_assign(consumer, partitions):
    """Trạng thái nằm trong bộ nhớ nên bỏ qua offset đã commit: đọc lại vài phút gần nhất (hoặc từ đầu)."""
    start = int((time.time() - REPLAY_MINUTES * 60) * 1000) if REPLAY_MINUTES else 0
    for p in partitions:
        p.offset = start                                     # timestamp (ms)
    consumer.assign(consumer.offsets_for_times(partitions))


def main():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "dnse-candles",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "linger.ms": 50})
    consumer.subscribe(["clean-trade", OHLC], on_assign=on_assign)
    count, last = 0, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error(msg.error())
                else:
                    try:
                        (add_ohlc if msg.topic() == OHLC else add)(json.loads(msg.value()))
                    except Exception:
                        log.exception("Bỏ qua bản ghi lỗi")
                    count += 1
            if count >= 2000 or (count and time.monotonic() - last > 5):
                flush(producer)
                count, last = 0, time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        flush(producer)
        consumer.close()


if __name__ == "__main__":
    main()