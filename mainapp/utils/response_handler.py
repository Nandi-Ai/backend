from rest_framework.response import Response
import logging

logger = logging.getLogger(__name__)


class ErrorResponse(Response):
    def __init__(self, message=None, error=None, status_code=500):
        super().__init__()
        if error:
            logger.exception(error)
        else:
            logger.error(message)
        self.status_code = status_code
        if message:
            self.data = {"error": str(message)}
        elif error:
            self.data = {"error": str(error)}


class UnimplementedErrorResponse(ErrorResponse):
    def __init__(self, message, error=None):
        super().__init__(message, error, status_code=501)


class BadRequestErrorResponse(ErrorResponse):
    def __init__(self, message, error=None):
        super().__init__(message, error, status_code=400)


class ForbiddenErrorResponse(ErrorResponse):
    def __init__(self, message, error=None):
        super().__init__(message, error, status_code=403)


class NotFoundErrorResponse(ErrorResponse):
    def __init__(self, message, error=None):
        super().__init__(message, error, status_code=404)


class ConflictErrorResponse(ErrorResponse):
    def __init__(self, message, error=None):
        super().__init__(message, error, status_code=409)
