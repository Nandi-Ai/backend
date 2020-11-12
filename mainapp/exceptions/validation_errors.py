class InvalidDatasetPermissions(Exception):
    pass


class InvalidDataSourceError(Exception):
    def __init__(self, error_response):
        super().__init__(error_response.data["error"])
        self.__error_response = error_response

    @property
    def error_response(self):
        return self.__error_response
