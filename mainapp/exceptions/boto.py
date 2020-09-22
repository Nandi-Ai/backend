class InvalidBotoResponse(Exception):
    def __init__(self, response):
        super().__init__("Invalid or unexpected response from boto")
        self.response = response
