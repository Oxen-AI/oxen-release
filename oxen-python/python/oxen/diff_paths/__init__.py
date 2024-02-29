"""
Oxen can be used to compare data frames and return a tabular diff.

```python
import os
import oxen

result = oxen.diff("dataset_1.csv", "dataset_2.csv")
print(result.diff)
```
"""

from ..oxen import PyDiff
from oxen.diff_paths.tabular_diff import TabularDiff
from oxen.diff_paths.text_diff import TextDiff


class Diff:
    """
    Diff class wraps the PyDiff class and helps convert to the variety of diff types.

    ```python
    import os
    import oxen

    result = oxen.diff("dataset_1.csv", "dataset_2.csv")
    print(result.diff)
    ```
    """

    def __init__(self, py_diff: PyDiff):
        self._py_diff = py_diff

    @property
    def format(self) -> str:
        """
        Returns the format of the diff. Ie. tabular, text, etc.
        """
        return self._py_diff.format

    @property
    def diff(self):
        """
        Returns the diff as the appropriate diff type.
        """
        match self._py_diff.format:
            case "tabular":
                return TabularDiff(self._py_diff.tabular)
            case "text":
                return TextDiff(self._py_diff.text)
            case "unknown":
                raise ValueError("The diff type is unknown.")
