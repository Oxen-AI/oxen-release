import oxen


class CreateLabelMap(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        lines = args[0].split("\n")
        canonical_labels = set(lines)
        data_labels = set(args[1])
        missing_labels = data_labels.difference(canonical_labels)
        if len(missing_labels) != 0:
            raise ValueError(
                f"Some label(s) in data missing from labels file: {missing_labels}"
            )
        return {line: i for i, line in enumerate(lines)}
