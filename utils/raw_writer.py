import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


class RawWriter:
    """Ghi mỗi message thành một dòng JSON (JSON Lines), đổi tệp theo giờ.

    Cấu trúc tệp: data/raw/{source}/{YYYY-MM-DD}/{HH}.jsonl (ngày và giờ theo múi giờ của sàn).

    Khóa JSON của mỗi bản ghi (trùng tên biến trong code):
        source                            nguồn dữ liệu
        topic                             loại dữ liệu (trade, ohlc_1_minute, ...)
        symbol                            mã chứng khoán
        event_timestamp_utc               thời gian sự kiện do sàn gửi, đã chuẩn hóa về UTC (có thể null)
        collector_received_timestamp_utc  thời điểm callback của collector nhận message, UTC
        sdk_received_timestamp_utc        thời điểm SDK nhận message (từ trường receivedAt trong payload), UTC
        sequence_number                   số thứ tự tăng dần của bản ghi
        payload                           nội dung gốc của message, không chỉnh sửa

    Dữ liệu thu trước đây dùng tên cũ: event_ts, recv_ts, sdk_recv_ts, seq (xem tools/inspect_raw.py).
    """

    def __init__(self, source, exchange_timezone="Asia/Ho_Chi_Minh", root_directory="data/raw"):
        self.source = source
        self.exchange_timezone = ZoneInfo(exchange_timezone)
        self.root_directory = Path(root_directory)
        self._write_lock = threading.Lock()     # lớp bảo hiểm; mô hình mới chỉ có một luồng ghi
        self._file_handle = None
        self._current_file_key = None

    def build_record(self, topic, payload, symbol=None, event_timestamp_utc=None,
                     sdk_received_timestamp_utc=None, sequence_number=None):
        """Tạo bản ghi (chạy ở producer, không đụng tới tệp). Trả về (file_key, record)."""
        current_utc_time = datetime.now(timezone.utc)
        record = {
            "source": self.source,
            "topic": topic,
            "symbol": symbol,
            "event_timestamp_utc": event_timestamp_utc,
            "collector_received_timestamp_utc": current_utc_time.isoformat(),
        }
        if sdk_received_timestamp_utc is not None:
            record["sdk_received_timestamp_utc"] = sdk_received_timestamp_utc
        if sequence_number is not None:
            record["sequence_number"] = sequence_number
        record["payload"] = payload

        local_time = current_utc_time.astimezone(self.exchange_timezone)
        file_key = (local_time.strftime("%Y-%m-%d"), local_time.strftime("%H"))
        return file_key, record

    def write_batch(self, keyed_records):
        """Ghi cả lô (danh sách (file_key, record)) rồi flush một lần."""
        if not keyed_records:
            return
        with self._write_lock:
            for file_key, record in keyed_records:
                if file_key != self._current_file_key:
                    self._rotate_file(file_key)
                try:
                    line = json.dumps(record, ensure_ascii=False, default=str)
                except (TypeError, ValueError):
                    # payload không mã hóa được: vẫn ghi dòng thay thế để không mất dấu vết
                    fallback_record = dict(record, payload=repr(record.get("payload")), payload_encoding_error=True)
                    line = json.dumps(fallback_record, ensure_ascii=False, default=str)
                self._file_handle.write(line + "\n")
            self._file_handle.flush()

    def close(self):
        with self._write_lock:
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
            self._current_file_key = None

    def _rotate_file(self, file_key):
        """Đóng tệp cũ (nếu có) và mở tệp của giờ mới ở chế độ ghi nối tiếp."""
        if self._file_handle:
            self._file_handle.close()
        folder_path = self.root_directory / self.source / file_key[0]
        folder_path.mkdir(parents=True, exist_ok=True)
        self._file_handle = open(folder_path / f"{file_key[1]}.jsonl", "a", encoding="utf-8")
        self._current_file_key = file_key