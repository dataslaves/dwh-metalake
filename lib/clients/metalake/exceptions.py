from typing import Any


class TrinoError(Exception):
    pass


class TrinoHttpError(TrinoError):
    def __init__(self, method: str, url: str, status: int, reason: str, body_preview: str = ""):
        super().__init__(f"{method} {url} failed: {status} {reason} {body_preview}")
        self.method = method
        self.url = url
        self.status = status
        self.reason = reason
        self.body_preview = body_preview


class TrinoValidationError(TrinoError):
    def __init__(self, message: str, doc: Any):
        super().__init__(message)
        self.doc = doc


class TrinoQueryError(TrinoError):
    def __init__(self, message: str, error: dict):
        super().__init__(message)
        self.error = error