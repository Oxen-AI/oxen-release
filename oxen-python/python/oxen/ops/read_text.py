import oxen


class ReadText(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        # TODO: actually read a file...
        return "Human: <REPLACE_ME> AI:"
