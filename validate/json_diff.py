from abc import ABC, abstractmethod


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
    return f"{hint}: -'{left}' +'{right}'"


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
        return f"MISS_{self.side}: '{self.value}'"

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
    else:
        raise NotImplementedError(f"Type {type(left)} is not supported.")

    return None
