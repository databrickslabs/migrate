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

    def test_set_diff(self):
        expected = DictDiff()
        expected.add_child('left', Miss('RIGHT', 'left'))
        expected.add_child('right', Miss('LEFT', 'right'))
        self.assertEqual(expected, diff_json({'left', 'common'},
                                             {'right', 'common'}))

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
                            children=DiffConfig(children={
                                'info': DiffConfig(primary_key='id')
                            })
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
                            children=DiffConfig(
                                ignored_keys={'value'},
                                children={
                                    'info': DiffConfig(
                                        primary_key='id',
                                        children=DiffConfig(
                                            ignored_keys={'v'}
                                        )
                                    )
                                })
                        )
                }))
        )


class PrintDiffTest(unittest.TestCase):
    def test_simple(self):
        expected1 = DictDiff()
        expected1.add_child('i', ValueDiff(1, 2))
        expected1.add_child('f', TypeDiff(2.0, 3))
        expected2 = DictDiff()
        expected2.add_child('s', ValueDiff('hello', 'world'))
        expected2.add_child('l', Miss('RIGHT', 'left'))
        expected2.add_child('r', Miss('LEFT', 'right'))
        expected1.add_child('n', expected2)
        print_diff(expected1, prefix="SIMPLE")
        # TODO(Yubing): Check output.
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
