import unittest
from threading_utils import propagate_exceptions
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

class MyBadException(Exception):
    pass

class ThreadingUtilsTest(unittest.TestCase):
    def test_should_propagate_exception(self):
        def do_something_good():
            return 'howdy'

        def do_something_bad():
            raise MyBadException('something bad happened')

        def run_stuff():
            fut1 = ThreadPoolExecutor(2).submit(do_something_good)
            fut2 = ThreadPoolExecutor(2).submit(do_something_bad)
            return fut1, fut2

        with self.assertRaises(MyBadException):
            futures = run_stuff()
            concurrent.futures.wait(futures)
            propagate_exceptions(futures)