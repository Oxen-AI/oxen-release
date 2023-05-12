import oxen


class ExtractCol(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        df, column = args
        return df[column]
