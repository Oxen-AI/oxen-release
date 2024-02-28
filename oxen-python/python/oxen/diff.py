from .oxen import diff

import os


def diff_tabular(
    left: os.PathLike, right: os.PathLike, keys: list[str], targets: list[str]
):
    """
    Compares two data frames and returns a tabular diff.

    Args:
        left: `os.PathLike`
            The left file to compare.
        right: `os.PathLike`
            The right file to compare.
        keys: `list[str]`
            The keys to compare on.
        targets: `list[str]`
            The targets to compare on.
    """
    return diff.diff_tabular(left, right, keys, targets)
