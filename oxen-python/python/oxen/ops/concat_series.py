import oxen
import polars as pl


class ConcatSeries(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        return pl.DataFrame(args)
