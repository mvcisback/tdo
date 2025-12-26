from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple


@dataclass
class CaldavConfig:
    calendar_url: str
    username: str
    password: str | None = None
    token: str | None = None


def _parse_config_file(path: Path) -> Iterable[Tuple[str, str]]:
    for raw in path.read_text().splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        yield key.lower(), value.strip()


def load_config(env: str | None = None, config_home: Path | None = None) -> CaldavConfig:
    env = env or os.environ.get("TODO_ENV") or "default"
    base_home = config_home or Path.home() / ".config" / "todo"
    values = {
        "calendar_url": os.environ.get("TODO_CALDAV_URL"),
        "username": os.environ.get("TODO_USERNAME"),
        "password": os.environ.get("TODO_PASSWORD"),
        "token": os.environ.get("TODO_TOKEN"),
    }
    config_path = base_home / f"config.{env}"
    if config_path.exists():
        for key, value in _parse_config_file(config_path):
            values[key] = value
    url = values.get("calendar_url")
    username = values.get("username")
    password = values.get("password")
    token = values.get("token")
    if not url or not username:
        raise RuntimeError("caldav configuration requires calendar_url and username")
    return CaldavConfig(calendar_url=url, username=username, password=password, token=token)
