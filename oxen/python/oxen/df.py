from .oxen import df

import os
from polars import DataFrame


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
