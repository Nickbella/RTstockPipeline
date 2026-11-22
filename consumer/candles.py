from datetime import datetime, time, timedelta

GRACE = timedelta(seconds=10)
ONE_MINUTE = timedelta(minutes=1)

UPSERT_SQL = """
INSERT INTO candles_1m (symbol, bucket, trading_date, open, high, low, close, volume, trade_count, sum_pv, vwap,
                        buy_volume, sell_volume, unknown_volume, cvd)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, bucket) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    sum_pv = EXCLUDED.sum_pv,
    vwap = EXCLUDED.vwap,
    buy_volume = EXCLUDED.buy_volume,
    sell_volume = EXCLUDED.sell_volume,
    unknown_volume = EXCLUDED.unknown_volume,
    cvd = EXCLUDED.cvd
"""

# Chỉ số trong list của mỗi nến
OPEN, HIGH, LOW, CLOSE, VOLUME, TRADE_COUNT, SUM_PV, BUY, SELL, UNKNOWN = range(10)


def is_continuous(moment):
    """Giờ khớp lệnh liên tục (không tính ATO, nghỉ trưa, ATC). Chưa xử lý ngày lễ."""
    t = moment.time()
    in_morning = time(9, 15) <= t < time(11, 30)
    in_afternoon = time(13, 0) <= t < time(14, 30)
    return moment.weekday() < 5 and (in_morning or in_afternoon)


class CandleBuilder:
    def __init__(self, connection):
        # {(symbol, bucket): [open, high, low, close, volume, trade_count, sum_pv,
        #                     buy_volume, sell_volume, unknown_volume]}
        self.candles = {}
        self.base = {}   # {symbol: (ngày, tổng delta của các nến đã rời RAM)} -> nền cho CVD
        self.last = {}   # {symbol: (giá khớp trước, chiều khớp trước)} cho tick rule
        self.changed_keys = set()
        self.watermark = None
        self.late_trade_count = 0

        max_bucket = connection.execute("SELECT max(bucket) FROM candles_1m").fetchone()[0]
        if max_bucket is None:
            return
        threshold = max_bucket - timedelta(minutes=2)

        query = """
            SELECT symbol, bucket, open, high, low, close, volume, trade_count, sum_pv,
                   buy_volume, sell_volume, unknown_volume
            FROM candles_1m
            WHERE bucket >= %s
        """
        for symbol, bucket, *values in connection.execute(query, (threshold,)):
            self.candles[(symbol, bucket)] = [float(v) for v in values]
            if self.watermark is None or bucket > self.watermark:
                self.watermark = bucket

        # Nền CVD: tổng delta các nến cũ hơn ngưỡng nạp, trong cùng ngày
        base_query = """
            SELECT symbol, sum(buy_volume - sell_volume)
            FROM candles_1m
            WHERE trading_date = %s AND bucket < %s
            GROUP BY symbol
        """
        for symbol, total in connection.execute(base_query, (threshold.date(), threshold)):
            self.base[symbol] = (threshold.date(), int(total))

    def classify(self, symbol, trade_time, price, side_field):
        """'buy' | 'sell' | None. Ưu tiên chiều do sàn gửi, nếu không thì tick rule.
        ATO/ATC là khớp định kỳ, không có bên chủ động -> None (vào unknown_volume)."""
        last_price, last_side = self.last.get(symbol, (None, None))
        side = None
        if is_continuous(trade_time):
            if side_field in ("buy", "sell"):
                side = side_field
            elif last_price is not None:
                if price > last_price:
                    side = "buy"
                elif price < last_price:
                    side = "sell"
                else:
                    side = last_side            # bằng giá trước: giữ chiều cũ
        self.last[symbol] = (price, side or last_side)
        return side

    def add(self, row):
        """Archiver cần gọi theo thứ tự thời gian của từng mã để tick rule đúng."""
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
            self.candles[key] = [price, price, price, price, 0, 0, 0.0, 0, 0, 0]

        side = self.classify(symbol, trade_time, price, row.get("side"))

        candle = self.candles[key]
        candle[HIGH] = max(candle[HIGH], price)
        candle[LOW] = min(candle[LOW], price)
        candle[CLOSE] = price
        candle[VOLUME] += volume
        candle[TRADE_COUNT] += 1
        candle[SUM_PV] += price * volume
        candle[BUY if side == "buy" else SELL if side == "sell" else UNKNOWN] += volume

        self.changed_keys.add(key)

    def cvd_for(self, symbol, bucket):
        """CVD = nền (các nến đã rời RAM) + delta các nến trong RAM từ đầu ngày đến hết bucket."""
        base_date, base = self.base.get(symbol, (None, 0))
        total = base if base_date == bucket.date() else 0
        for (s, b), candle in self.candles.items():
            if s == symbol and b <= bucket and b.date() == bucket.date():
                total += int(candle[BUY] - candle[SELL])
        return total

    def write(self, connection):
        if not self.changed_keys:
            return

        # Nến đổi làm CVD của các nến sau nó (cùng mã) đổi theo -> ghi lại luôn các nến đó
        earliest = {}
        for symbol, bucket in self.changed_keys:
            earliest[symbol] = min(bucket, earliest.get(symbol, bucket))
        keys = [k for k in self.candles if k[0] in earliest and k[1] >= earliest[k[0]]]

        rows = []
        for symbol, bucket in keys:
            (open_p, high_p, low_p, close_p, volume, trade_count, sum_pv,
             buy_volume, sell_volume, unknown_volume) = self.candles[(symbol, bucket)]
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
                int(buy_volume),
                int(sell_volume),
                int(unknown_volume),
                self.cvd_for(symbol, bucket),
            ))

        with connection.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, rows)

    def done(self):
        self.changed_keys.clear()
        if self.watermark is None:
            return

        expired_limit = self.watermark - GRACE - ONE_MINUTE
        expired_keys = sorted((k for k in self.candles if k[1] < expired_limit), key=lambda k: k[1])

        for key in expired_keys:
            symbol, bucket = key
            candle = self.candles.pop(key)
            base_date, base = self.base.get(symbol, (None, 0))
            if base_date != bucket.date():          # sang ngày mới: CVD về 0
                base = 0
            self.base[symbol] = (bucket.date(), base + int(candle[BUY] - candle[SELL]))