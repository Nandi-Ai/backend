class UnableToGetGlueColumns(Exception):
    def __init__(self):
        super().__init__("Unable to get glue column")