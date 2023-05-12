import oxen


class ReadDF(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        # print(f"read_df {args}")
        return oxen.util.read_df(args[0])
