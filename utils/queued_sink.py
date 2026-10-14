import logging
import queue
import threading

log = logging.getLogger("dnse.sink")
_STOP = object()   # poison pill


# Class này dữ liệu vào file JSONL theo lô, dùng queue để gom các bản ghi lại rồi ghi một lần 
class QueuedSink:
    def __init__(self, writer, maxsize=100_000, batch_size=500):
        self.writer = writer
        self.q = queue.Queue(maxsize=maxsize)
        self.batch_size = batch_size
        self.dropped = 0        # Đếm số lượng bản ghi bị bỏ qua, tăng khi số bản ghi bị bỏ tăng
        self.written = 0        # Số lượng bản ghi đã ghi vào file 
        self._thread = threading.Thread(target=self._run, daemon=True) # Luồng chạy nền để ghi dữ liệu vào file JSONL

    def start(self):
        self._thread.start()

    # Gửi một bản ghi vào queue, nếu queue đầy thì bỏ qua và đếm số lượng bản ghi bị bỏ
    def put(self, item):
        try:
            self.q.put_nowait(item)      # Đẩy bản ghi vào queue liên tục đến khi queue đầy thì bỏ qua
        except queue.Full:
            self.dropped += 1            # Queue đầy, tăng biến đếm số lượng bản ghi bị bỏ qua

    def stop(self, timeout=30):
        self.q.put(_STOP)
        self._thread.join(timeout)       # Chờ luồng ghi dừng lại, nếu timeout thì log cảnh báo lỗi
        if self._thread.is_alive():
            log.error("Luồng ghi chưa dừng sau %ss, còn ~%d bản ghi", timeout, self.q.qsize())
        self.writer.close()

    def _run(self):
        stopping = False
        while not stopping:
            batch = [self.q.get()]                       # Lấy một bản ghi từ queue, nếu queue rỗng thì chờ
            while len(batch) < self.batch_size:          # Nếu số lượng bản ghi trong lô chưa đủ batch_size thì tiếp tục lấy thêm từ queue
                # Lấy liên tục các bản ghi từ queue, nếu queue rỗng thì dừng
                try:
                    batch.append(self.q.get_nowait())    
                except queue.Empty:
                    break
            # Nếu bản ghi cuối cùng trong lô là _STOP thì dừng luồng ghi
            if batch[-1] is _STOP:                       # _STOP luôn là phần tử cuối cùng
                batch.pop()
                stopping = True
            # Nếu lô có bản ghi thì ghi vào file JSONL
            if batch:
                try:
                    self.writer.write_many(batch)        
                    self.written += len(batch)
                except Exception:
                    log.exception("Lỗi ghi lô %d bản ghi", len(batch))