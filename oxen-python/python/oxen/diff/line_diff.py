from ..oxen import PyLineDiff, PyChangeType

from oxen.diff.change_type import ChangeType


class LineDiff:
    """
    A class representing a change in a line of text.
    """

    def __init__(self, diff: PyLineDiff):
        self._diff = diff

    def __repr__(self) -> str:
        return (
            f"LineDiff(modification={self._diff.modification}, text={self._diff.text})"
        )

    @property
    def modification(self) -> ChangeType:
        """
        Returns the modification of the line diff.
        """
        match self._diff.modification:
            case PyChangeType.Added:
                return ChangeType.ADDED
            case PyChangeType.Removed:
                return ChangeType.REMOVED
            case PyChangeType.Modified:
                return ChangeType.MODIFIED
            case PyChangeType.Unchanged:
                return ChangeType.UNCHANGED
            case _:
                raise ValueError(f"Invalid modification: {self._diff.modification}")

    @property
    def text(self) -> str:
        """
        Returns the text of the line diff.
        """
        return self._diff.text
