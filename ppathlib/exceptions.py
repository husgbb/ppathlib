"""Custom exceptions used by the `ppathlib` prototype."""


class PPathLibError(Exception):
    """Base exception for all ppathlib prototype errors."""


class PPathLibWarning(Warning):
    """Base warning for ppathlib runtime caveats."""


class ExperimentalRemoteRuntimeWarning(PPathLibWarning, RuntimeWarning):
    """Warning emitted when experimental remote runtime behavior is used."""


class InvalidConfigurationException(PPathLibError, ValueError):
    """Raised when profile configuration or path inputs are invalid."""
