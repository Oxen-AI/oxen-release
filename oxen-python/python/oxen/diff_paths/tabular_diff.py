from ..oxen import PyTabularDiff

from polars import DataFrame


class TabularDiff:
    def __init__(self, diff: PyTabularDiff):
        self._diff = diff

    def __repr__(self) -> str:
        return f"TabularDiff(shape={self._diff.data.shape})\n{self._diff.data}"

    @property
    def data(self) -> DataFrame:
        """
        Returns the data of the diff as a polars data frame.
        """
        return self._diff.data
