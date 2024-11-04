class DatabricksDBFSMissingFiles(RuntimeError):
    def __init__(
        self,
        message,
    ):
        super().__init__(message)


class AuthenticationError(RuntimeError):
    def __init__(
        self,
        message,
    ):
        super().__init__(message)
