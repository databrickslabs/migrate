import threading

global_lock = threading.Lock()

class ThreadSafeWriter():
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

    def write(self, data):
        with global_lock:
            self.filewriter.write(data)

    def close(self):
        self.filewriter.close()
        