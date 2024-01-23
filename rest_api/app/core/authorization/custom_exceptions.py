from fastapi import WebSocketException, status


class BadCredentialsException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1008_POLICY_VIOLATION, reason="Bad credentials")


class RequiresAuthenticationException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1008_POLICY_VIOLATION, reason="Requires authentication")


class UnableCredentialsException(WebSocketException):
    def __init__(self):
        super().__init__(code=status.WS_1011_INTERNAL_ERROR, reason="Unable to verify credentials")