from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


__all__ = ["cli", "config", "caldav_client", "models", "update_parser"]


try:
    __version__ = version("todo")
except PackageNotFoundError:
    __version__ = "0.1.0"
