import sys

from .remote_path import RemoteBindingRequest, RemotePath, RemoteProfileClient
from .ppath import clear_client_cache, get_client, PPath


if sys.version_info[:2] >= (3, 8):
    import importlib.metadata as importlib_metadata
else:
    import importlib_metadata


try:
    __version__ = importlib_metadata.version(__name__.split(".", 1)[0])
except importlib_metadata.PackageNotFoundError:
    __version__ = "0.1.0"


__all__ = [
    "RemoteBindingRequest",
    "RemotePath",
    "RemoteProfileClient",
    "PPath",
    "get_client",
    "clear_client_cache",
]
