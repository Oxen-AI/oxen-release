from enum import Enum


class ChangeType(Enum):
    """
    An enum representing the type of change in a line diff.
    """

    ADDED = "Added"
    REMOVED = "Removed"
    MODIFIED = "Modified"
    UNCHANGED = "Unchanged"
