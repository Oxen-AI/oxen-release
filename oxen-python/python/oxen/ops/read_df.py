import oxen


class ReadDF(oxen.Op):
    """
    Reads a polars DataFrame from a file.

    Args:
        args[0] : str
            File path to read DataFrame from
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        # print(f"read_df {args}")
        return oxen.util.read_df(args[0])
