"""Transport-neutral application errors with stable machine-readable codes."""

from __future__ import annotations


class ApplicationError(Exception):
    def __init__(self, message: str, *, code: str = "application_error") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class ValidationError(ApplicationError):
    def __init__(self, message: str, *, code: str = "invalid_request") -> None:
        super().__init__(message, code=code)


class NotFound(ApplicationError):
    def __init__(self, message: str, *, code: str = "not_found") -> None:
        super().__init__(message, code=code)


class Conflict(ApplicationError):
    def __init__(self, message: str, *, code: str = "conflict") -> None:
        super().__init__(message, code=code)
