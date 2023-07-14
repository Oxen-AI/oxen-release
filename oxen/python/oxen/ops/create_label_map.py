import oxen


class CreateLabelMap(oxen.Op):
    """
    Creates a dictionary mapping string labels to integers,
    based on a canonical line-separated text file of labels.

    Args:
        args[0]: str
            String of line-separated labels
        args[1]: List[str]
            Iterable of labels in data, as a check against unexpected data values
    """

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
