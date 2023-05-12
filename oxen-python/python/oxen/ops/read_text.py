import oxen
from pathlib import Path


class ReadText(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        file = args[0]
        return Path(file).read_text()
