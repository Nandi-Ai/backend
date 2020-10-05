class InvalidEventData(Exception):
    def __init__(self, err_message):
        super().__init__(err_message)
