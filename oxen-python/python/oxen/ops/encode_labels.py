import oxen
import numpy as np


class EncodeLabels(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        return np.array(args[0].map_dict(args[1]))
