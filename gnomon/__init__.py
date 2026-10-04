"""Gnomon: a storage engine for bi-temporal property graphs."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _installed_version

from gnomon.core.model import OPEN_END, EntityRef, Interval
from gnomon.store import Store, open

try:
    #: Read from the installed distribution rather than restated here. A
    #: literal drifted silently from 0.1.0 through four releases, because
    #: nothing reads it at build time and nothing tested it.
    __version__ = _installed_version("gnomon")
except PackageNotFoundError:  # running from a source tree, not installed
    __version__ = "0.0.0+unknown"

__all__ = ["OPEN_END", "EntityRef", "Interval", "Store", "open"]
