class QueueFileBroken(Exception):
    """Raised when queue file is modified or corrupted."""

    def __init__(self, message: str = "Queue file is modified or corrupted."):
        super().__init__(message)
