import sys

from .exceptions import ExperimentalRemoteRuntimeWarning, InvalidConfigurationException
from .ppath import clear_client_cache, PPath


if sys.version_info[:2] >= (3, 8):
    import importlib.metadata as importlib_metadata
else:
    import importlib_metadata


try:
    __version__ = importlib_metadata.version(__name__.split(".", 1)[0])
except importlib_metadata.PackageNotFoundError:
    __version__ = "0.0.0"


__all__ = [
    "PPath",
    "ExperimentalRemoteRuntimeWarning",
    "InvalidConfigurationException",
    "clear_client_cache",
]
