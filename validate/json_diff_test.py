import unittest
from .json_diff import *


class JsonDiffTest(unittest.TestCase):
    def test_equal(self):
        self.assertEqual(None, diff_json(1, 1))
        self.assertEqual(None, diff_json(2.0, 2.0))
        self.assertEqual(None, diff_json('hello world', 'hello world'))
        self.assertEqual(None, diff_json({'i': 1, 'f': 2.0, 's': 'hello world'},
                                         {'i': 1, 'f': 2.0, 's': 'hello world'}))

    def test_value_diff(self):
        self.assertEqual(ValueDiff(1, 2), diff_json(1, 2))
        self.assertEqual(ValueDiff(1.0, 2.0), diff_json(1.0, 2.0))
        self.assertEqual(ValueDiff('hello', 'world'), diff_json('hello', 'world'))

    def test_type_diff(self):
        self.assertEqual(TypeDiff(1, 2.0), diff_json(1, 2.0))
        self.assertEqual(TypeDiff('hello', 3), diff_json('hello', 3))
        self.assertEqual(TypeDiff(2.0, {}), diff_json(2.0, {}))
        self.assertEqual(TypeDiff({}, 1), diff_json({}, 1))

    def test_dict_diff(self):
        expected = DictDiff()
        expected.add_child('i', ValueDiff(1, 2))
        expected.add_child('f', TypeDiff(2.0, 3))
        expected.add_child('s', ValueDiff('hello', 'world'))
        expected.add_child('l', Miss('RIGHT', 'left'))
        expected.add_child('r', Miss('LEFT', 'right'))
        self.assertEqual(expected, diff_json({'i': 1, 'f': 2.0, 's': 'hello', 'l': 'left'},
                                             {'f': 3, 's': 'world', 'i': 2, 'r': 'right'}))

    def test_nested_dict_diff(self):
        expected1 = DictDiff()
        expected1.add_child('i', ValueDiff(1, 2))
        expected1.add_child('f', TypeDiff(2.0, 3))

        expected2 = DictDiff()
        expected2.add_child('s', ValueDiff('hello', 'world'))
        expected2.add_child('l', Miss('RIGHT', 'left'))
        expected2.add_child('r', Miss('LEFT', 'right'))
        expected1.add_child('n', expected2)
        self.assertEqual(expected1, diff_json(
            {'i': 1, 'f': 2.0, 'e': 'equal', 'n': {'s': 'hello', 'l': 'left'}},
            {'f': 3, 'i': 2, 'e': 'equal', 'n': {'s': 'world', 'r': 'right'}}))


if __name__ == '__main__':
    unittest.main()
