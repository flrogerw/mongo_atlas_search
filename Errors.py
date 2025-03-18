class QuarantineError(Exception):
    """Exception raised when a record needs to be quarantined due to validation or processing issues."""

    def __init__(self, message: str):
        """Initialize the exception with an error message."""
        super().__init__(message)


class ValidationError(Exception):
    """Exception raised when data validation fails."""

    def __init__(self, message: str):
        """Initialize the exception with an error message."""
        super().__init__(message)
