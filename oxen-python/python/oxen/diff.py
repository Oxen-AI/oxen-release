from .oxen import diff

import os
from typing import Optional

# TODO: make the interface take the same arguments as the CLI
# Optional:
# - repo_dir: os.PathLike = "."
# - revision_left: Option[str] = None
# - revision_right: Option[str] = None
# - output: Option[os.PathLike] = None
def diff_tabular(
    left: os.PathLike,
    right: os.PathLike,
    keys: list[str],
    targets: list[str],
    repo_dir: Optional[os.PathLike] = None,
    revision_left: Optional[str] = None,
    revision_right: Optional[str] = None,
    output: Optional[os.PathLike] = None,
):
    """
    Compares two data frames and returns a tabular diff.

    Args:
        left: `os.PathLike`
            The left path to compare.
        right: `os.PathLike`
            The right path to compare.
        keys: `list[str]`
            The keys to compare on. This is used to join the two data frames. Keys will be combined and hashed to create a identifier for each row.
        targets: `list[str]`
            The targets to compare on. This is used to compare the values of the two data frames.
    """
    return diff.diff_tabular(left, right, keys, targets)
