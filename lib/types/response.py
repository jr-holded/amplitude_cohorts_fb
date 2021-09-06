from typing import NamedTuple

class Response(NamedTuple):
    success: bool
    message: str
    data: object = None