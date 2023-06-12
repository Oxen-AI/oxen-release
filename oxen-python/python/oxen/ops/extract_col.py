import oxen


class ExtractCol(oxen.Op):
    def __init__(self, *args, **kwargs):
        """
        Extracts a column from a DataFrame.

        Args:
            args[0]: polars.DataFrame
                DataFrame to extract column from
            args[1]: str
                Name of column to extract
        """
        super().__init__(*args, **kwargs)

    def call(self, args):
        df, column = args
        return df[column]
