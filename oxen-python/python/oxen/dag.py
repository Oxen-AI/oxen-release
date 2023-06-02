class DAG:
    def __init__(self, outputs=None):
        self.outputs = outputs or []

    def evaluate(self):
        visited = set()
        outputs = []

        def _dfs(node):
            if node not in visited:
                visited.add(node)
                for parent in node.parents:
                    _dfs(parent)
                outputs.append(node)

        for output_node in self.outputs:
            _dfs(output_node)

        desired_output_ids = [node.id for node in self.outputs]
        results = [None] * len(self.outputs)
        for node in outputs:
            result = node.run()
            if node.id in desired_output_ids:
                results[desired_output_ids.index(node.id)] = result

        return results
