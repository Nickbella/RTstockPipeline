"""Dựng nến + VWAP từ Trade đã sạch. VWAP = Σ(giá×KL) / ΣKL (đơn vị giá theo đầu vào: nghìn đồng)."""
from collections import defaultdict
from datetime import datetime, time, timedelta
from typing import NamedTuple
from zoneinfo import ZoneInfo

VN = ZoneInfo("Asia/Ho_Chi_Minh")
SESSIONS = (("ATO", time(9, 0), time(9, 15)), ("CONTINUOUS", time(9, 15), time(11, 30)),
            ("CONTINUOUS", time(13, 0), time(14, 30)), ("ATC", time(14, 30), time(14, 45)))


class Trade(NamedTuple):
    symbol: str
    time: datetime            # có múi giờ
    price: float
    volume: int
    tiebreak: tuple = ()


class Candle(NamedTuple):
    symbol: str
    bucket: datetime          # đầu khung, giờ VN
    session: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    value: float
    vwap: float | None
    trades: int


def session_of(t):
    t = t.astimezone(VN).time()
    return next((name for name, a, b in SESSIONS if a <= t < b), None)


def floor_time(t, seconds):
    t = t.astimezone(VN)
    midnight = t.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((t - midnight).total_seconds())
    return midnight + timedelta(seconds=elapsed - elapsed % seconds)


def _merge(symbol, bucket, session, rows):
    """rows: (open, high, low, close, volume, value, count), đã sắp theo thời gian."""
    volume, value = sum(r[4] for r in rows), sum(r[5] for r in rows)
    return Candle(symbol, bucket, session, rows[0][0], max(r[1] for r in rows), min(r[2] for r in rows),
                  rows[-1][3], volume, value, value / volume if volume else None, sum(r[6] for r in rows))


def build_candles(trades, seconds=60):
    """Trade -> nến. Sắp theo thời gian sự kiện nên open/close không phụ thuộc thứ tự đến."""
    groups = defaultdict(list)
    for t in sorted(trades, key=lambda t: (t.time, t.tiebreak)):
        session = session_of(t.time)
        if session and t.price > 0 and t.volume > 0:     # loại thỏa thuận/ngoài giờ/dữ liệu xấu
            groups[(t.symbol, floor_time(t.time, seconds), session)].append(
                (t.price, t.price, t.price, t.price, t.volume, t.price * t.volume, 1))
    return sorted((_merge(*key, rows) for key, rows in groups.items()), key=lambda c: (c.bucket, c.symbol))


def aggregate(candles, seconds):
    """Nến nhỏ -> nến lớn (5m, 1h, 1d...). Không lấy trung bình VWAP."""
    groups = defaultdict(list)
    for c in sorted(candles, key=lambda c: c.bucket):
        groups[(c.symbol, floor_time(c.bucket, seconds))].append(c)
    out = []
    for (symbol, bucket), g in groups.items():
        sessions = {c.session for c in g}
        out.append(_merge(symbol, bucket, sessions.pop() if len(sessions) == 1 else "MIXED",
                          [(c.open, c.high, c.low, c.close, c.volume, c.value, c.trades) for c in g]))
    return sorted(out, key=lambda c: (c.bucket, c.symbol))


def cumulative_vwap(candles):
    """[(nến, VWAP lũy kế trong ngày)], reset mỗi ngày và mỗi mã."""
    total, out = defaultdict(lambda: [0, 0.0]), []
    for c in sorted(candles, key=lambda c: c.bucket):
        t = total[(c.symbol, c.bucket.date())]
        t[0] += c.volume
        t[1] += c.value
        out.append((c, t[1] / t[0] if t[0] else None))
    return out