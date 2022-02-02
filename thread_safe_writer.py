from queue import Queue, Empty
from threading import Thread

class ThreadSafeWriter:
    """Class that ensures the thread-safe file write via the Synchronized Queue object.
    For example, this class can be used by multiple threads to write to the same file safely.
    This class is not useful when parallelization is done across multiple files.

    Initialize by passing in the file open args.
    e.g. writer = ThreadSafeWriter("file_to_write.txt", "w")
         writer.write("content1")
         writer.write("content2")
         writer.close()
    """
    def __init__(self, *args):
        self.filewriter = open(*args)
        self.queue = Queue()
        self.finished = False
        # Single thread of actually writing to a file.
        Thread(name = "ThreadSafeWriter", target=self.internal_writer).start()
    def write(self, data):
        self.queue.put(data)
    def internal_writer(self):
        while not self.finished:
            if not self.queue.empty():
                data = self.queue.get(block=True)
                self.filewriter.write(data)
                self.filewriter.flush()
                self.queue.task_done()
    def close(self):
        self.queue.join()
        self.finished = True
        self.filewriter.close()

# def fn(writer, data):
#   time.sleep(5)
#   writer.write(data + " ")
#
# with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
#   futures = {executor.submit(fn, writer, str(data)): data for data in range(20)}
#   for future in concurrent.futures.as_completed(futures):
#     dl_resp = future.result()

#use it like ordinary open like this:
# w = SafeWriter("filename", "w")
# w.write("can be used among multiple threads")
# w.close() #it is really important to close or the program would not end