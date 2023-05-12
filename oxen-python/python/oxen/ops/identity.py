import oxen


class Identity(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        # print(f"identity_fn {args}")
        return self.input
