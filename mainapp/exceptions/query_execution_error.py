class QueryExecutionError(Exception):
    def __init__(self):
        super().__init__("Query execution failed")


class InvalidExecutionId(Exception):
    def __init__(self):
        super().__init__("Result ID execution failed")


class MaxExecutionReactedError(Exception):
    def __init__(self):
        super().__init__("Reached to max query executions")


class UnsupportedColumnTypeError(Exception):
    def __init__(self):
        super().__init__("The column type is unsupported")
