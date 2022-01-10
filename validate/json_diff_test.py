import unittest
from .json_diff import *
from collections import defaultdict

init_diff_logger("/tmp")


class JsonDiffTest(unittest.TestCase):
    def test_equal(self):
        counters = defaultdict(int)
        self.assertEqual(None, diff_json(1, 1, counters))
        self.assertEqual(None, diff_json(2.0, 2.0, counters))
        self.assertEqual(None, diff_json('hello world', 'hello world', counters))
        self.assertEqual(None, diff_json({'i': 1, 'f': 2.0, 's': 'hello world'},
                                         {'i': 1, 'f': 2.0, 's': 'hello world'}, counters))
        self.assertFalse(counters)

    def test_value_diff(self):
        counters = defaultdict(int)
        expected_counters = defaultdict(int)
        self.assertEqual(ValueDiff(1, 2, expected_counters), diff_json(1, 2, counters))
        self.assertEqual(ValueDiff(1.0, 2.0, expected_counters), diff_json(1.0, 2.0, counters))
        self.assertEqual(ValueDiff('hello', 'world', expected_counters), diff_json('hello', 'world', counters))
        self.assertEqual(counters, expected_counters)
        self.assertEqual(dict(counters), {'VALUE_MISMATCH': 3})

    def test_type_diff(self):
        counters = defaultdict(int)
        expected_counters = defaultdict(int)
        self.assertEqual(TypeDiff(1, 2.0, expected_counters), diff_json(1, 2.0, counters))
        self.assertEqual(TypeDiff('hello', 3, expected_counters), diff_json('hello', 3, counters))
        self.assertEqual(TypeDiff(2.0, {}, expected_counters), diff_json(2.0, {}, counters))
        self.assertEqual(TypeDiff({}, 1, expected_counters), diff_json({}, 1, counters))
        self.assertEqual(counters, expected_counters)
        self.assertEqual(dict(counters), {'TYPE_MISMATCH': 4})

    def test_dict_diff(self):
        counters = defaultdict(int)
        expected_counters = defaultdict(int)
        expected = DictDiff()
        expected.add_child('i', ValueDiff(1, 2, expected_counters))
        expected.add_child('f', TypeDiff(2.0, 3, expected_counters))
        expected.add_child('s', ValueDiff('hello', 'world', expected_counters))
        expected.add_child('l', Miss('DESTINATION', 'source', expected_counters))
        expected.add_child('r', Miss('SOURCE', 'destination', expected_counters))
        self.assertEqual(expected, diff_json({'i': 1, 'f': 2.0, 's': 'hello', 'l': 'source'},
                                             {'f': 3, 's': 'world', 'i': 2, 'r': 'destination'},
                                             counters))
        self.assertEqual(counters, expected_counters)
        self.assertEqual(dict(counters), {
            'TYPE_MISMATCH': 1, 'VALUE_MISMATCH': 2, 'MISS_DESTINATION': 1, 'MISS_SOURCE': 1})

    def test_set_diff(self):
        counters = defaultdict(int)
        expected_counters = defaultdict(int)
        expected = DictDiff()
        expected.add_child('source', Miss('DESTINATION', 'source', expected_counters))
        expected.add_child('destination', Miss('SOURCE', 'destination', expected_counters))
        self.assertEqual(expected, diff_json({'source', 'common'},
                                             {'destination', 'common'}, counters))
        self.assertEqual(counters, expected_counters)
        self.assertEqual(dict(counters), {'MISS_DESTINATION': 1, 'MISS_SOURCE': 1})

    def test_nested_dict_diff(self):
        counters = defaultdict(int)
        expected_counters = defaultdict(int)
        expected1 = DictDiff()
        expected1.add_child('i', ValueDiff(1, 2, expected_counters))
        expected1.add_child('f', TypeDiff(2.0, 3, expected_counters))

        expected2 = DictDiff()
        expected2.add_child('s', ValueDiff('hello', 'world', expected_counters))
        expected2.add_child('l', Miss('DESTINATION', 'source', expected_counters))
        expected2.add_child('r', Miss('SOURCE', 'destination', expected_counters))
        expected1.add_child('n', expected2)
        self.assertEqual(expected1, diff_json(
            {'i': 1, 'f': 2.0, 'e': 'equal', 'n': {'s': 'hello', 'l': 'source'}},
            {'f': 3, 'i': 2, 'e': 'equal', 'n': {'s': 'world', 'r': 'destination'}}, counters))
        self.assertEqual(counters, expected_counters)
        self.assertEqual(dict(counters), {
            'TYPE_MISMATCH': 1, 'VALUE_MISMATCH': 2, 'MISS_DESTINATION': 1, 'MISS_SOURCE': 1})


class PrepareDiffInputTest(unittest.TestCase):
    def test_no_change(self):
        self.assertEqual(1, prepare_diff_input(1))
        self.assertEqual(2.0, prepare_diff_input(2.0))
        self.assertEqual('hello', prepare_diff_input('hello'))
        self.assertEqual({}, prepare_diff_input({}))

    def test_prime_list(self):
        self.assertEqual({}, prepare_diff_input([]))
        self.assertEqual({1, 2, 3}, prepare_diff_input([1, 2, 3]))
        self.assertEqual({1.0, 2.0, 3.0}, prepare_diff_input([1.0, 2.0, 3.0]))
        self.assertEqual({'1', '3', '2'}, prepare_diff_input(['1', '2', '3']))

    def test_list_of_dict(self):
        self.assertEqual(
            {'b': {'key': 'b', 'value': 'y'},
             'a': {'key': 'a', 'value': 'q'},
             'c': {'key': 'c', 'value': 'n'}},
            prepare_diff_input(
                [{'key': 'b', 'value': 'y'},
                 {'key': 'c', 'value': 'n'},
                 {'key': 'a', 'value': 'q'}],
                DiffConfig(
                    primary_key='key'
                )))

    def test_simple_nested(self):
        self.assertEqual(
            {
                'foo': {
                    'b': {'key': 'b', 'value': 'y'},
                    'a': {'key': 'a', 'value': 'q'},
                    'c': {'key': 'c', 'value': 'n'}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [{'key': 'b', 'value': 'y'},
                            {'key': 'c', 'value': 'n'},
                            {'key': 'a', 'value': 'q'}],
                    'bar': 'baz'
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key='key'
                        )
                })))

    def test_deep_nested(self):
        self.assertEqual(
            {
                'foo': {
                    'b': {'key': 'b', 'value': 'y', 'info': {100: {'id': 100, 'v': '111'}}},
                    'a': {'key': 'a', 'value': 'q', 'info': {200: {'id': 200, 'v': '222'}}},
                    'c': {'key': 'c', 'value': 'n', 'info': {300: {'id': 300, 'v': '333'}}}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [
                        {'key': 'b', 'value': 'y', 'info': [{'id': 100, 'v': '111'}]},
                        {'key': 'c', 'value': 'n', 'info': [{'id': 300, 'v': '333'}]},
                        {'key': 'a', 'value': 'q', 'info': [{'id': 200, 'v': '222'}]},
                    ],
                    'bar': 'baz',
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key='key',
                            children={
                                'info': DiffConfig(primary_key='id')
                            }
                        )
                }))
        )

    def test_nested_primary_key(self):
        self.assertEqual(
            {
                'foo': {
                    100: {'key': 'b', 'value': 'y', 'info': {'id': 100, 'v': '111'}},
                    200: {'key': 'a', 'value': 'q', 'info': {'id': 200, 'v': '222'}},
                    300: {'key': 'c', 'value': 'n', 'info': {'id': 300, 'v': '333'}}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [
                        {'key': 'b', 'value': 'y', 'info': {'id': 100, 'v': '111'}},
                        {'key': 'c', 'value': 'n', 'info': {'id': 300, 'v': '333'}},
                        {'key': 'a', 'value': 'q', 'info': {'id': 200, 'v': '222'}},
                    ],
                    'bar': 'baz',
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key={'info': 'id'},
                        )
                }))
        )

    def test_hash_key(self):
        self.maxDiff = None
        self.assertEqual(
            {
                'foo': {
                    "{'key': 'b', 'value': 'y', 'info': {100: {'id': 100, 'v': '111'}}}":
                        {'key': 'b', 'value': 'y', 'info': {100: {'id': 100, 'v': '111'}}},
                    "{'key': 'a', 'value': 'q', 'info': {200: {'id': 200, 'v': '222'}}}":
                        {'key': 'a', 'value': 'q', 'info': {200: {'id': 200, 'v': '222'}}},
                    "{'key': 'c', 'value': 'n', 'info': {300: {'id': 300, 'v': '333'}}}":
                        {'key': 'c', 'value': 'n', 'info': {300: {'id': 300, 'v': '333'}}}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [
                        {'key': 'b', 'value': 'y', 'info': [{'id': 100, 'v': '111'}]},
                        {'key': 'c', 'value': 'n', 'info': [{'id': 300, 'v': '333'}]},
                        {'key': 'a', 'value': 'q', 'info': [{'id': 200, 'v': '222'}]},
                    ],
                    'bar': 'baz',
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key='__HASH__',
                            children={
                                'info': DiffConfig(primary_key='id')
                            }
                        )
                }))
        )

    def test_multiple_keys(self):
        self.assertEqual(
            {
                'foo': {
                    'b': {'key': 'b', 'value': 'y', 'info': {100: {'id': 100, 'v': '111'}}},
                    'a': {'key': 'a', 'value': 'q', 'info': {200: {'id': 200, 'v': '222'}}},
                    'c': {'key2': 'c', 'value': 'n', 'info': {300: {'id': 300, 'v': '333'}}}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [
                        {'key': 'b', 'value': 'y', 'info': [{'id': 100, 'v': '111'}]},
                        {'key2': 'c', 'value': 'n', 'info': [{'id': 300, 'v': '333'}]},
                        {'key': 'a', 'value': 'q', 'info': [{'id': 200, 'v': '222'}]},
                    ],
                    'bar': 'baz',
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key=['key', 'key2'],
                            children={
                                'info': DiffConfig(primary_key='id')
                            }
                        )
                }))
        )

    def test_ignore(self):
        self.assertEqual(
            {
                'foo': {
                    'b': {'key': 'b', 'info': {100: {'id': 100}}},
                    'a': {'key': 'a', 'info': {200: {'id': 200}}},
                    'c': {'key': 'c', 'info': {300: {'id': 300}}}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [
                        {'key': 'b', 'value': 'y', 'info': [{'id': 100, 'v': '111'}]},
                        {'key': 'c', 'value': 'n', 'info': [{'id': 300, 'v': '333'}]},
                        {'key': 'a', 'value': 'q', 'info': [{'id': 200, 'v': '222'}]},
                    ],
                    'bar': 'baz',
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key='key',
                            ignored_keys={'value'},
                            children={
                                'info': DiffConfig(
                                    primary_key='id',
                                    ignored_keys={'v'}
                                )
                            }
                        )
                }))
        )

    def test_duplicates(self):
        self.assertEqual(
            {
                'foo': {
                    'b': {'key': 'b', 'value': 'y'},
                    'a': {'key': 'a', 'value': 'q'},
                    'c': {'key': 'c', 'value': 'n'}
                },
                'bar': 'baz'
            },
            prepare_diff_input(
                {
                    'foo': [{'key': 'b', 'value': 'y'},
                            {'key': 'c', 'value': 'n'},
                            {'key': 'c', 'value': 'm'},
                            {'key': 'a', 'value': 'q'}],
                    'bar': 'baz'
                },
                DiffConfig(children={
                    'foo':
                        DiffConfig(
                            primary_key='key'
                        )
                })))


class PrintDiffTest(unittest.TestCase):
    def test_simple(self):
        logging.basicConfig(level=logging.DEBUG, format="", force=True)
        counters = defaultdict(int)
        expected1 = DictDiff()
        expected1.add_child('i', ValueDiff(1, 2, counters))
        expected1.add_child('f', TypeDiff(2.0, 3, counters))
        expected2 = DictDiff()
        expected2.add_child('s', ValueDiff('hello', 'world', counters))
        expected2.add_child('l', Miss('DESTINATION', 'source', counters))
        expected2.add_child('r', Miss('SOURCE', 'destination', counters))
        expected1.add_child('n', expected2)
        print_diff(expected1, prefix="SIMPLE")
        # TODO(Yubing): Check output.
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
