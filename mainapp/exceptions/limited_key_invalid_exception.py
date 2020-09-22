class LimitedKeyInvalidException(Exception):
    def __init__(self, data_source):
        super().__init__(f"Limited key in dataset {data_source.dataset.id} is invalid")
