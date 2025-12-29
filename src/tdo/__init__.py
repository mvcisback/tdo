from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


__all__ = ["cli", "config", "caldav_client", "models", "sqlite_cache", "update_parser", "time_parser"]


try:
    __version__ = version("tdo")
except PackageNotFoundError:
    __version__ = "0.1.0"
