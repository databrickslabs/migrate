from abc import ABC, abstractmethod
from dataclasses import dataclass, field


class AbstractDiff(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def __str__(self):
        pass


class Diff(AbstractDiff):
    def __init__(self, left, right):
        super().__init__()
        self.left = left
        self.right = right

    @abstractmethod
    def __str__(self):
        pass


def _diff_message(hint, left, right):
    return f"{hint}:\n< {left}\n---\n> {right}"


class TypeDiff(Diff):
    def __init__(self, left, right):
        super().__init__(left, right)

    def __str__(self):
        return _diff_message("TYPE_MISMATCH", f"{type(self.left)}':'{self.left}",
                             f"{type(self.right)}':'{self.right}")

    def __eq__(self, other):
        return str(self) == str(other)


class ValueDiff(Diff):
    def __init__(self, left, right):
        super().__init__(left, right)

    def __str__(self):
        return _diff_message("VALUE_MISMATCH", self.left, self.right)

    def __eq__(self, other):
        return str(self) == str(other)


class DictDiff(AbstractDiff):
    def __init__(self):
        super().__init__()
        self.children = {}

    def __str__(self):
        return str({key: str(self.children[key]) for key in sorted(self.children.keys())})

    def __eq__(self, other):
        return str(self) == str(other)

    def add_child(self, key, diff):
        assert key not in self.children
        self.children[key] = diff

    def has_diff(self):
        return len(self.children) > 0


class Miss(AbstractDiff):
    def __init__(self, side, value):
        super().__init__()
        self.side = side
        self.value = value

    def __str__(self):
        hint = f"MISS_{self.side}:\n"
        value = f"< {self.value}" if self.side == "RIGHT" else f"> {self.value}"
        return f"{hint}{value}"

    def __eq__(self, other):
        return str(self) == str(other)


def diff_json(left, right):
    """diff_json compares two dict and return the diff.

    It is required that input dicts only contains dict and prime data types. List is not supported
    intentionally to reduce the complexity.

    :param left - left hand side of the comparison.
    :param right - right hand side of the comparison.
    """
    if type(left) != type(right):
        return TypeDiff(left, right)

    if isinstance(left, (int, float, str)):
        if left != right:
            return ValueDiff(left, right)
    elif isinstance(left, dict):
        diff = DictDiff()
        for key in set(left.keys()).union(set(right.keys())):
            if key not in left:
                diff.add_child(key, Miss('LEFT', right[key]))
            elif key not in right:
                diff.add_child(key, Miss('RIGHT', left[key]))
            else:
                child_diff = diff_json(left[key], right[key])
                if child_diff:
                    diff.add_child(key, child_diff)
        if diff.has_diff():
            return diff
    elif isinstance(left, set):
        diff = DictDiff()
        for key in left.union(right):
            if key not in left:
                diff.add_child(key, Miss('LEFT', key))
            elif key not in right:
                diff.add_child(key, Miss('RIGHT', key))
        if diff.has_diff():
            return diff
    else:
        raise NotImplementedError(f"Type {type(left)} is not supported.")

    return None


class DiffConfig:
    """
    DiffConfig configures how to diff two Python dicts. It should have the same structure as `data`,
    and follow the the rules below:
    1) If `data` is a list of dicts, primary key is required and the children should be another
    DiffConfig object which holds config for inner dict.
    2) If `data` is a dict, ignore_keys can be set to ignore certain fields. And the children should
    be a dict which holds config for children.

    """

    def __init__(self, primary_key=None, ignored_keys=None, children=None):
        self.primary_key = primary_key
        self.ignore_keys = ignored_keys
        self.children = children


def prepare_diff_input(data, config=None):
    """ This function converts input data with the two rules below:
    1) List of primary types to sets.
    2) List of dict to dict of dicts keyed by primary key.

    :param data: on side of input for diffing.
    :param config: an instance of DiffConfig in order to specify the primary key of dicts
    within a list. It should follow the same dict structure of data. See `test_deep_nested` in
    json_diff_test.py for example.
    :return: the converted data which should be ready to be fed into diff_json.
    """
    if isinstance(data, list):
        if len(data) == 0:
            return {}
        elif isinstance(data[0], (int, float, str)):
            return set(data)
        elif isinstance(data[0], dict):
            assert isinstance(config, DiffConfig) and config.primary_key, \
                f"Config missing for {str(data)}"
            result = {}
            for inner in data:
                converted = prepare_diff_input(inner, config.children)
                result[converted[config.primary_key]] = converted
            return result
        else:
            raise NotImplementedError(f"Type {type(data[0])} is not supported.")
    elif isinstance(data, dict):
        if len(data) == 0:
            return {}
        assert not config or isinstance(config, DiffConfig)
        result = {}
        for key, value in data.items():
            if config and config.ignore_keys and key in config.ignore_keys:
                continue
            else:
                child_config = None
                if config and config.children:
                    child_config = config.children.get(key, None)
                result[key] = prepare_diff_input(value, child_config)

        return result
    elif isinstance(data, (int, float, str)):
        return data
    else:
        raise NotImplementedError(f"Type {type(data)} is not supported.")


def print_diff(diff, prefix=""):
    if not diff:
        print("No diff found.")
    elif isinstance(diff, (TypeDiff, ValueDiff, Miss)):
        print(prefix + ":" + str(diff) + "\n")
    elif isinstance(diff, DictDiff):
        for key, value in diff.children.items():
            print_diff(value, prefix + "|" + key)
    else:
        raise NotImplementedError(f"Type {type(diff)} is not supported.")
