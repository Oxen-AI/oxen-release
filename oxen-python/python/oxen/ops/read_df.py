import oxen
import polars as pl


class ReadDF(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        # TODO: actually read a file...
        return pl.DataFrame(
            {
                "prompt": [
                    "what?",
                    "who?",
                    "where?",
                ],
                "response": ["blue", "slim shady", "here"],
            }
        )
