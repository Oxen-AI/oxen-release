import oxen
from pathlib import Path


class ReadText(oxen.Op):
    """
    Reads a text file

    Args:
        args[0] : str
            File path to read text from
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        file = args[0]
        return Path(file).read_text()
