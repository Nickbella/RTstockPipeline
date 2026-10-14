from datetime import datetime, timezone
from zoneinfo import ZoneInfo

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")


def to_utc_iso_string(timestamp):
    """Nhận epoch (giây/mili giây/micro giây/nano giây) hoặc chuỗi thời gian -> chuỗi ISO 8601 UTC."""
    if timestamp is None:
        return None

    if isinstance(timestamp, str):
        stripped_text = timestamp.strip()
        try:
            timestamp = float(stripped_text)            # chuỗi dạng số, ví dụ "1759716903123"
        except ValueError:                              # không phải số -> coi là chuỗi ngày giờ
            parsed_datetime = datetime.fromisoformat(stripped_text.replace("Z", "+00:00"))
            if parsed_datetime.tzinfo is None:          # không ghi múi giờ -> giả định giờ Việt Nam
                parsed_datetime = parsed_datetime.replace(tzinfo=VIETNAM_TIMEZONE)
            return parsed_datetime.astimezone(timezone.utc).isoformat()

    epoch_seconds = float(timestamp)
    if epoch_seconds > 1e17:        # nano giây
        epoch_seconds /= 1e9
    elif epoch_seconds > 1e14:      # micro giây
        epoch_seconds /= 1e6
    elif epoch_seconds > 1e11:      # mili giây
        epoch_seconds /= 1e3
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


def safe_to_utc_iso_string(timestamp):
    """Phiên bản an toàn: không đổi được thì trả về None thay vì báo lỗi."""
    try:
        return to_utc_iso_string(timestamp)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def to_vietnam_time_string(utc_iso_string, format_pattern="%Y-%m-%d %H:%M:%S"):
    """Chỉ để hiển thị: chuỗi UTC -> chuỗi giờ Việt Nam."""
    return datetime.fromisoformat(utc_iso_string).astimezone(VIETNAM_TIMEZONE).strftime(format_pattern)