from datetime import datetime, timezone
from zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def to_utc_iso(ts):
    """Nhận số epoch (giây/ms/µs/ns) hoặc chuỗi thời gian -> chuỗi ISO 8601 UTC."""
    if ts is None:
        return None

    if isinstance(ts, str):
        s = ts.strip()
        try:
            ts = float(s)                       # chuỗi dạng số, ví dụ "1759716903123"
        except ValueError:                      # không phải số -> coi là chuỗi ISO
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:               # chuỗi không ghi múi giờ -> giả định giờ VN
                dt = dt.replace(tzinfo=VN_TZ)
            return dt.astimezone(timezone.utc).isoformat()

    ts = float(ts)
    if ts > 1e17:      # nano giây
        ts /= 1e9
    elif ts > 1e14:    # micro giây
        ts /= 1e6
    elif ts > 1e11:    # mili giây
        ts /= 1e3
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def safe_utc_iso(ts):
    """Phiên bản an toàn: parse hỏng thì trả về None thay vì báo lỗi."""
    try:
        return to_utc_iso(ts)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def to_vn_str(utc_iso, fmt="%Y-%m-%d %H:%M:%S"):
    """Chỉ để hiển thị: chuỗi UTC -> chuỗi giờ VN."""
    return datetime.fromisoformat(utc_iso).astimezone(VN_TZ).strftime(fmt)