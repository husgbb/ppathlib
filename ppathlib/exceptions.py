"""Custom exceptions used by the `ppathlib` prototype."""


class PPathLibError(Exception):
    """Base exception for all ppathlib prototype errors."""


class InvalidConfigurationException(PPathLibError, ValueError):
    """Raised when profile configuration or path inputs are invalid."""
