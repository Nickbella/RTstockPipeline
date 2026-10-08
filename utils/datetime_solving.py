# fromisoformat: Chuyển chuỗi thành object datetime     
# isoformat: Chuyển object datetime thành chuỗi 

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Khai báo biến múi giờ Việt Nam
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Hàm này nhận thời gian (dạng chuỗi/số nguyên) và chuyển về dạng chuỗi ngày, tháng, năm tiêu chuẩn 
def to_utc_iso(ts): 
    if ts is None:
        return None

    # Nếu thời gian nhận về ở dạng chuỗi ký tự
    if isinstance(ts, str):
        s = ts.strip() # Khai báo biến mới và gán giá trị là chuỗi được xóa khoảng trắng ở hai đầu
        try:
            ts = float(s)                       # Ép kiểu chuỗi về dạng số thực
        except ValueError:                      # không phải số -> coi là chuỗi ISO
            dt = datetime.fromisoformat(s.replace("Z", "+00:00")) # Z = giờ UTC gốc, chuyển thành dạng +00:00 
            if dt.tzinfo is None:               # Nếu chuỗi không có thông tin múi giờ, mặc định là múi giờ Việt Nam
                dt = dt.replace(tzinfo=VN_TZ)
            return dt.astimezone(timezone.utc).isoformat() # Trả về chuỗi thời gian tiêu chuẩn dạng UTC

    # Trường hợp thời gian nhận về không đồng nhất ở dạng đơn vị giây, mili giây, micro giây, nano giây 
    ts = float(ts)
    if ts > 1e17:      # Chuyển đơn vị nano giây về giây
        ts /= 1e9
    elif ts > 1e14:    # Chuyển đơn vị micro giây về giây
        ts /= 1e6
    elif ts > 1e11:    # Chuyển đơn vị mili giây về giây
        ts /= 1e3
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() # Trả về chuỗi thời gian tiêu chuẩn dạng UTC

# Nếu có lỗi thì trả về None, không dừng chương trình
def safe_utc_iso(ts):
    try:
        return to_utc_iso(ts)
    except (ValueError, TypeError, OverflowError, OSError):
        return None

# Hiển thị thời gian theo múi giờ Việt Nam 
def to_vn_str(utc_iso, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.fromisoformat(utc_iso).astimezone(VN_TZ).strftime(fmt)