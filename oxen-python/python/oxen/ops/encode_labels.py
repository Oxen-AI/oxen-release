import oxen
import numpy as np


class EncodeLabels(oxen.Op):
    """
    Maps a column of labels to a column of integers according to a dictionary.

    Args:
        args[0] : polars.Series
            Polars series of labels to be mapped
        args[1] : Dict[str, int]
            Dictionary mapping labels to integers
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        return np.array(args[0].map_dict(args[1]))
