import uuid


class Op:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.id = uuid.uuid4()
        self.input = None
        self.name = self.__class__.__name__

        if "name" in kwargs:
            self.name = kwargs["name"]

        if "input" in kwargs:
            self.input = kwargs["input"]

        # print(f"Creating op {self.name}(args={self.args}, kwargs={self.kwargs})")

        self.parents = []

    def __repr__(self):
        return f"{self.name}({self.args})"

    # Links to the parent Operations that need to run first
    def __call__(self, *args):
        for arg in args:
            # print(f"  {self} --parent--> {arg}")
            self.parents.append(arg)
        return self

    # For the child op to implement
    def call(self, _args):
        raise NotImplementedError()

    # Combines the data, the args, and the parent inputs, and computes the output
    def run(self):
        # print("=" * 5)
        # print(f"Running {self}")
        # print(f"parents {self.parents}")

        # these will be the inputs to the subsequent node call
        inputs = []
        if self.input:
            inputs.append(self.input)
        if self.args:
            inputs.append(self.args)
        if self.parents:
            inputs.extend([node.input for node in self.parents])

        # transform the inputs into the output
        self.input = self.call(inputs)
        # print(f"output {self.input}")
        # print("=" * 5)
        return self.input
