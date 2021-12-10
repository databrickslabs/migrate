import os.path
from abc import ABC, abstractmethod
import logging


class AbstractDiff(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def __str__(self):
        pass


class Diff(AbstractDiff):
    def __init__(self, source, destination):
        super().__init__()
        self.source = source
        self.destination = destination

    @abstractmethod
    def __str__(self):
        pass


def _diff_message(hint, source, destination):
    return f"{hint}:\n< {source}\n---\n> {destination}"


class TypeDiff(Diff):
    def __init__(self, source, destination):
        super().__init__(source, destination)

    def __str__(self):
        return _diff_message("TYPE_MISMATCH", f"{type(self.source)}':'{self.source}",
                             f"{type(self.destination)}':'{self.destination}")

    def __eq__(self, other):
        return str(self) == str(other)


class ValueDiff(Diff):
    def __init__(self, source, destination):
        super().__init__(source, destination)

    def __str__(self):
        return _diff_message("VALUE_MISMATCH", self.source, self.destination)

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
        value = f"< {self.value}" if self.side == "DESTINATION" else f"> {self.value}"
        return f"{hint}{value}"

    def __eq__(self, other):
        return str(self) == str(other)


def diff_json(source, destination):
    """diff_json compares two dict and return the diff.

    It is required that input dicts only contains dict and prime data types. List is not supported
    intentionally to reduce the complexity.

    :param source - source hand side of the comparison.
    :param destination - destination hand side of the comparison.
    """
    if type(source) != type(destination):
        return TypeDiff(source, destination)

    if isinstance(source, (int, float, str)):
        if source != destination:
            return ValueDiff(source, destination)
    elif isinstance(source, dict):
        diff = DictDiff()
        for key in set(source.keys()).union(set(destination.keys())):
            if key not in source:
                diff.add_child(key, Miss('SOURCE', destination[key]))
            elif key not in destination:
                diff.add_child(key, Miss('DESTINATION', source[key]))
            else:
                child_diff = diff_json(source[key], destination[key])
                if child_diff:
                    diff.add_child(key, child_diff)
        if diff.has_diff():
            return diff
    elif isinstance(source, set):
        diff = DictDiff()
        for key in source.union(destination):
            if key not in source:
                diff.add_child(key, Miss('SOURCE', key))
            elif key not in destination:
                diff.add_child(key, Miss('DESTINATION', key))
        if diff.has_diff():
            return diff
    else:
        raise NotImplementedError(f"Type {type(source)} is not supported.")

    return None


class DiffConfig:
    """
    DiffConfig configures how to diff two Python dicts. It should have the same structure as `data`,
    and follow the rules below:
    1) If `data` is a list of dicts, primary key is required. And config object itself also holds
     config for inner dict.
    2) If `data` is a dict, ignore_keys can be set to ignore certain fields. And the children should
    be a dict which holds config for children.

    """

    def __init__(self, primary_key=None, ignored_keys=None, children=None):
        self.primary_key = primary_key
        self.ignore_keys = ignored_keys
        self.children = children


_diff_logger = logging.getLogger('validate')


def init_diff_logger(base_dir):
    _diff_logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(base_dir, "validation.log"))
    fh.setLevel(logging.INFO)
    _diff_logger.addHandler(fh)


def diff_logger():
    return _diff_logger


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
                converted = prepare_diff_input(inner, config)
                if converted[config.primary_key] not in result:
                    result[converted[config.primary_key]] = converted
                else:
                    diff_logger().info(f"Duplicates found:\n{str(converted)}\n---\n" +
                                       str(result[converted[config.primary_key]]))
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
        diff_logger().info("No diff found.")
    elif isinstance(diff, (TypeDiff, ValueDiff, Miss)):
        diff_logger().info(prefix + ":" + str(diff) + "\n")
    elif isinstance(diff, DictDiff):
        for key in sorted(diff.children.keys()):
            value = diff.children[key]
            print_diff(value, prefix + "|" + key)
    else:
        raise NotImplementedError(f"Type {type(diff)} is not supported.")
