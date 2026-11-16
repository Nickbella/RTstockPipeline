from datetime import datetime, timedelta

GRACE = timedelta(seconds=10)
ONE_MINUTE = timedelta(minutes=1)

UPSERT_SQL = """
INSERT INTO candles_1m (symbol, bucket, trading_date, open, high, low, close, volume, trade_count, sum_pv, vwap)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, bucket) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    sum_pv = EXCLUDED.sum_pv,
    vwap = EXCLUDED.vwap
"""


class CandleBuilder:
    def __init__(self, connection):
        self.candles = {}  # {(symbol, bucket): [open, high, low, close, volume, trade_count, sum_pv]}
        self.changed_keys = set()
        self.watermark = None
        self.late_trade_count = 0

        query = """
            SELECT symbol, bucket, open, high, low, close, volume, trade_count, sum_pv
            FROM candles_1m
            WHERE bucket >= (SELECT max(bucket) FROM candles_1m) - interval '2 minutes'
        """
        for symbol, bucket, *values in connection.execute(query):
            self.candles[(symbol, bucket)] = [float(v) for v in values]
            if self.watermark is None or bucket > self.watermark:
                self.watermark = bucket

    def add(self, row):
        symbol = row["symbol"]
        trade_time = datetime.fromisoformat(row["time_vn"])
        price = float(row["price"])
        volume = int(row["volume"])

        bucket = trade_time.replace(second=0, microsecond=0)
        key = (symbol, bucket)

        if self.watermark is None or trade_time > self.watermark:
            self.watermark = trade_time

        if key not in self.candles:
            candle_deadline = bucket + ONE_MINUTE + GRACE
            if candle_deadline < self.watermark:
                self.late_trade_count += 1
                return
            self.candles[key] = [price, price, price, price, 0, 0, 0.0]

        candle = self.candles[key]
        candle[1] = max(candle[1], price)  # high
        candle[2] = min(candle[2], price)  # low
        candle[3] = price                  # close
        candle[4] += volume                # volume
        candle[5] += 1                     # trade_count
        candle[6] += price * volume        # sum_pv

        self.changed_keys.add(key)

    def write(self, connection):
        if not self.changed_keys:
            return

        rows = []
        for symbol, bucket in self.changed_keys:
            open_p, high_p, low_p, close_p, volume, trade_count, sum_pv = self.candles[(symbol, bucket)]
            vwap = round(sum_pv / volume, 3) if volume else None

            rows.append((
                symbol,
                bucket,
                bucket.date(),
                open_p,
                high_p,
                low_p,
                close_p,
                int(volume),
                int(trade_count),
                round(sum_pv, 3),
                vwap,
            ))

        with connection.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, rows)

    def done(self):
        self.changed_keys.clear()
        if self.watermark is None:
            return

        expired_limit = self.watermark - GRACE - ONE_MINUTE
        expired_keys = [key for key in self.candles if key[1] < expired_limit]

        for key in expired_keys:
            del self.candles[key]