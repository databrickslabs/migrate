import unittest
import filecmp
import os
from thread_safe_writer import ThreadSafeWriter
from threading_utils import propagate_exceptions
import concurrent.futures

class ThreadSafeWriterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.makedirs('test/thread_safe_writer', exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        os.rmdir('test/thread_safe_writer')

    def test_write_with_thread_safe_writer(self):
        f1 = "test/thread_safe_writer/test_file_1.log"
        f2 = "test/thread_safe_writer/test_file_2.log"

        list_to_write = [i for i in range(1000)]
        with open(f1, "w") as write_fp:
            for data in list_to_write:
                write_fp.write(str(data) + "\n")

        file_writer = ThreadSafeWriter(f2, "w")
        for data in list_to_write:
            file_writer.write(str(data) + "\n")
        file_writer.close()
        assert(filecmp.cmp(f1, f2))
        os.remove(f1)
        os.remove(f2)

    def test_write_with_thread_safe_writer_multithread(self):
        f1 = "test/thread_safe_writer/test_file_3.log"
        f2 = "test/thread_safe_writer/test_file_4.log"
        list_to_write = [i for i in range(10000)]
        with open(f1, "w") as write_fp:
            for data in list_to_write:
                write_fp.write(str(data) + "\n")

        file_writer = ThreadSafeWriter(f2, "w")
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(file_writer.write, str(data) + "\n") for data in list_to_write]
            concurrent.futures.wait(futures)
            propagate_exceptions(futures)

        file_writer.close()

        fp1 = open(f1, "r")
        fp2 = open(f2, "r")
        f1_lines = fp1.readlines()
        f2_lines = fp2.readlines()
        fp1.close()
        fp2.close()

        # since it is multi thread writing to the same file, the order is not guaranteed.
        # hence we test the content equality by sorting and then comparing.
        assert(not filecmp.cmp(f1, f2))
        assert(f1_lines.sort() == f2_lines.sort())
        os.remove(f1)
        os.remove(f2)
