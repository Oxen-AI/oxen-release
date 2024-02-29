"""
Oxen can be used to compare data frames and return a tabular diff.

```python
import os
import oxen

result = oxen.diff("dataset_1.csv", "dataset_2.csv")
print(result.diff)
```
"""

from ..oxen import diff
from oxen import df
from oxen.diff_paths import Diff

import os
from typing import Optional


def diff_paths(
    path: os.PathLike,
    to: Optional[os.PathLike] = None,
    keys: list[str] = [],
    targets: list[str] = [],
    repo_dir: Optional[os.PathLike] = None,
    revision_left: Optional[str] = None,
    revision_right: Optional[str] = None,
    output: Optional[os.PathLike] = None,
):
    """
    Compares data from two paths and returns a diff respecting the type of data.

    Args:
        path: `os.PathLike`
            The path to diff. If `compare_to` is not provided,
            this will compare the data frame to the previous commit.
        to: `os.PathLike`
            The path to compare. If provided this will be the right side of the diff.
        keys: `list[str]`
            The keys to compare on. This is used to join the two data frames.
            Keys will be combined and hashed to create a identifier for each row.
        targets: `list[str]`
            The targets to compare on. This is used to compare the values of the
            two data frames.
        repo_dir: `os.PathLike`
            The path to the oxen repository. Must be provided if `compare_to` is
            not provided, or if `revision_left` or `revision_right` is provided.
        revision_left: `str`
            The left revision to compare. Can be a commit hash or branch name.
        revision_right: `str`
            The right revision to compare. Can be a commit hash or branch name.
        output: `os.PathLike`
            The path to save the diff to. If not provided, the diff will not be saved.
    """
    result = diff.diff_paths(
        path, keys, targets, to, repo_dir, revision_left, revision_right
    )
    if output:
        df.save(result, output)
    return Diff(result)
