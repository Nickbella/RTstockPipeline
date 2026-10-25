import logging
import queue
import threading

logger = logging.getLogger("dnse.sink")
_STOP_SIGNAL = object()   # poison pill: nằm sau mọi bản ghi đã vào queue


class QueuedSink:
    """Callback gọi enqueue_record() (không chặn); một luồng riêng gom bản ghi rồi ghi qua batch_writer.write_batch().

    Chỉ luồng này chạm vào tệp nên không cần khóa. queue.Queue đã tự an toàn giữa các luồng.
    """

    def __init__(self, batch_writer, maximum_queue_size=100_000, maximum_batch_size=500):
        self.batch_writer = batch_writer
        self.record_queue = queue.Queue(maxsize=maximum_queue_size)
        self.maximum_batch_size = maximum_batch_size
        self.dropped_record_count = 0
        self.written_record_count = 0
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)

    def start(self):
        self._writer_thread.start()

    def enqueue_record(self, keyed_record):
        try:
            self.record_queue.put_nowait(keyed_record)      # không bao giờ chặn event loop
        except queue.Full:
            self.dropped_record_count += 1                  # queue đầy thì bỏ và đếm

    def stop(self, timeout_seconds=30):
        """Gọi sau khi producer đã dừng. Ghi nốt phần còn lại rồi đóng tệp."""
        self.record_queue.put(_STOP_SIGNAL)
        self._writer_thread.join(timeout_seconds)
        if self._writer_thread.is_alive():
            logger.error("Luồng ghi chưa dừng sau %ss, còn khoảng %d bản ghi",
                         timeout_seconds, self.record_queue.qsize())
        self.batch_writer.close()

    def _writer_loop(self):
        is_stopping = False
        while not is_stopping:
            record_batch = [self.record_queue.get()]                 # chờ đến khi có dữ liệu
            while len(record_batch) < self.maximum_batch_size:       # gom thêm những gì đang sẵn có
                try:
                    record_batch.append(self.record_queue.get_nowait())
                except queue.Empty:
                    break
            if record_batch[-1] is _STOP_SIGNAL:                     # tín hiệu dừng luôn là phần tử cuối
                record_batch.pop()
                is_stopping = True
            if record_batch:
                try:
                    self.batch_writer.write_batch(record_batch)        # ghi cả lô rồi flush một lần
                    self.written_record_count += len(record_batch)
                except Exception:
                    logger.exception("Lỗi ghi lô %d bản ghi", len(record_batch))