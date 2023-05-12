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

        results = []
        for node in outputs:
            print(f"node {node}")
            result = node.run()
            if node in self.outputs:
                results.append(result)

        return results
