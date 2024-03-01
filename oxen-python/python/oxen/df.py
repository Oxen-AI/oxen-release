"""
The `df` module provides a consistent interface for working with data frames.

Supported types: csv, parquet, json, jsonl, arrow

Example usage:

```python
import os
from oxen import df

# load a data frame
data_frame = df.load("path/to/data.csv")

# save a data frame
df.save(data_frame, "path/to/save.csv")
```
"""

from .oxen import df

import os
import sys
from polars import DataFrame


class load_df_call:
    """
    Reads a file into a data frame. The file format is inferred from the file extension.

    Supported types: csv, parquet, json, jsonl, arrow
    """

    def __call__(self, path: os.PathLike):
        return load(path)


def load(
    path: os.PathLike,
):
    """
    Reads a file into a data frame. The file format is inferred from the file extension.

    Supported types: csv, parquet, json, jsonl, arrow

    Args:
        path: `os.PathLike`
            The path to the file to read.
    """
    return df.load(path)


def save(
    data_frame: DataFrame,
    path: os.PathLike,
):
    """
    Saves a data frame to a file. The file format is inferred from the file extension.

    Args:
        data_frame: `DataFrame`
            The polars data frame to save.
        path: `os.PathLike`
            The path to save the data frame to.
    """
    return df.save(data_frame, path)


sys.modules[__name__] = load_df_call()
