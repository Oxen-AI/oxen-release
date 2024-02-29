from ..oxen import PyTextDiff, PyLineDiff, PyChangeType

from oxen.diff_paths.change_type import ChangeType


class LineDiff:
    """
    A class representing a change in a line of text.

    + Added
    - Removed
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


class TextDiff:
    def __init__(self, diff: PyTextDiff):
        self._diff = diff

    def __repr__(self) -> str:
        return f"TextDiff(num_added={self.num_added}, num_removed={self.num_removed})"

    def __str__(self) -> str:
        # iterate over lines and print them with a + or - prefix
        return "\n".join([f"{line.value}" for line in self._diff.lines])

    @property
    def num_added(self) -> int:
        """
        Returns the number of added lines in the diff.
        """
        # count the number of added lines
        return self._count_lines(PyChangeType.Added)

    @property
    def num_removed(self) -> int:
        """
        Returns the number of removed lines in the diff.
        """
        # count the number of removed lines
        return self._count_lines(PyChangeType.Removed)

    @property
    def lines(self) -> list[LineDiff]:
        """
        Returns the contents of the diff as a polars data frame.
        """
        # map the PyLineDiff to LineDiff
        return [LineDiff(line) for line in self._diff.lines]

    def _count_lines(self, modification: PyChangeType) -> int:
        return len(
            [line for line in self._diff.lines if line.modification == modification]
        )
