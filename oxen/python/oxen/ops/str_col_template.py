import oxen


class StrColTemplate(oxen.Op):
    def __init__(self, *args, **kwargs):
        self.search = kwargs["search"] if "search" in kwargs else "{prompt}"
        super().__init__(*args, **kwargs)

    def call(self, args):
        value, column = args
        result = column.map_elements(lambda x: value.replace(self.search, x))
        return result
