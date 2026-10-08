import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


# Ghi mỗi message nhận được vào file jsonl, mỗi file tên theo cấu trúc: data/raw/{source}/{YYYY-MM-DD}/{HH}.jsonl
class RawWriter:
    def __init__(self, source, exchange_tz="Asia/Ho_Chi_Minh",
                 root="data/raw", flush_every=50, flush_seconds=5):
        self.source = source # Nguồn dữ liệu: DNSE
        self.tz = ZoneInfo(exchange_tz) # Múi giờ sàn giao dịch: VN
        self.root = Path(root)
        self.flush_every = flush_every # Số lượng message nhận được thì xả một lần vào file JSONL: 50
        self.flush_seconds = flush_seconds # Thời gian chờ tối đa giữa các lần xả: 5 giây
        self._lock = threading.Lock() # Khóa luồng để tránh xung đột khi nhiều luồng cùng ghi dữ liệu vào file JSONL
        self._fh = None # Tạo biến file handle để mở/đổi file JSONL theo ngày và giờ
        self._current_key = None # Tạo khóa cập nhật thời gian, dùng để xác định xem có cần tạo file mới theo ngày và giờ hay không
        self._count = 0 # Đếm số lượng message nhận được, dùng để xác định khi nào xả dữ liệu vào file JSONL
        self._last_flush = time.monotonic() # Lưu thời gian xả dữ liệu cuối cùng, dùng để xác định khi nào xả dữ liệu vào file JSONL

    # Định nghĩa nội dung 1 message được ghi vào file JSONL theo giờ 
    def write(self, topic, payload, symbol=None, event_ts=None):
        now_utc = datetime.now(timezone.utc) # Khai báo biến lấy thời gian hiện tại theo múi giờ UTC0
        record = {
            "source": self.source, # Nguồn: Mặc định DNSE
            "topic": topic, # Loại message: trade, quote, orderbook, ohlc...
            "symbol": symbol, # Mã chứng khoán
            "event_ts": event_ts, # Thời gian sự kiện
            "recv_ts": now_utc.isoformat(), # Thời gian nhận được message (dạng chuỗi, múi giờ UTC0)
            "payload": payload, # Nội dung gốc của message
        }

        # Chuyển đổi dict record thành dạng chuỗi, đảm bảo in ra toàn vẹn văn bản và xuống dòng sau mỗi dict
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"

        # Chuyển đổi thời gian nhận được message sang múi giờ sàn giao dịch, và tạo key theo cấu trúc (YYYY-MM-DD, HH)
        local = now_utc.astimezone(self.tz) # Khai báo biến chuyển giờ UTC0 sang giờ sàn giao dịch (VN) dùng cho key 
        key = (local.strftime("%Y-%m-%d"), local.strftime("%H")) # Khóa quyết định xem có cần tạo file mới theo ngày và giờ hay không

        # Thao tác với khóa luồng
        with self._lock:
            if key != self._current_key: # Nếu giờ/ngày trên khóa mới khác với khóa hiện tại, thì tạo file mới theo ngày và giờ
                self._rotate(key)
            self._fh.write(line) # Nhận dòng chuỗi chuyển đổi từ dict record 
            self._count += 1 # Đếm số lượng message mới
            # Nếu số message đếm đạt 50 hoặc thời gian hiện tại trôi qua 5 giây từ lần xả cuối thì xả dữ liệu vào file
            if (self._count % self.flush_every == 0
                    or time.monotonic() - self._last_flush >= self.flush_seconds):
                self._fh.flush() # Xả dữ liệu vào file JSONL
                self._last_flush = time.monotonic() # Cập nhật thời gian xả dữ liệu cuối cùng

    # Hàm tạo file mới theo ngày và giờ, đóng file cũ nếu có
    def _rotate(self, key):
        if self._fh: # Đóng file cũ đang mở nếu có 
            self._fh.close()
        folder = self.root / self.source / key[0] # Tạo thư mục biểu thị ngày theo cấu trúc: data/raw/{source}/{YYYY-MM-DD}
        folder.mkdir(parents=True, exist_ok=True) # Tạo thư mục nếu chưa tồn tại, không báo lỗi nếu đã tồn tại
        self._fh = open(folder / f"{key[1]}.jsonl", "a", encoding="utf-8") # Đặt tên file theo cấu trúc (nâng giờ), mở file để ghi
        self._current_key = key # Cập nhật khóa hiện tại theo ngày và giờ mới

    # Đóng file JSONL khi không còn ghi dữ liệu nữa 
    def close(self):
        with self._lock: # Thao tác với khóa luồng
            if self._fh: # Nếu file đang mở thì đóng file
                self._fh.close() 
                self._fh = None