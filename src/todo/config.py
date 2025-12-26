from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple


@dataclass
class CaldavConfig:
    calendar_url: str
    username: str
    password: str | None = None
    token: str | None = None


def resolve_env(env: str | None = None) -> str:
    return env or os.environ.get("TODO_ENV") or "default"


def config_file_path(env: str | None = None, config_home: Path | None = None) -> Path:
    base_home = config_home or Path.home() / ".config" / "todo"
    resolved_env = resolve_env(env)
    return base_home / f"config.{resolved_env}.toml"


def write_config_file(path: Path, config: CaldavConfig, *, force: bool = False) -> Path:
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["[caldav]"]
    lines.append(f"calendar_url = {json.dumps(config.calendar_url)}")
    lines.append(f"username = {json.dumps(config.username)}")
    if config.password:
        lines.append(f"password = {json.dumps(config.password)}")
    if config.token:
        lines.append(f"token = {json.dumps(config.token)}")
    path.write_text("\n".join(lines) + "\n")
    return path


def _parse_config_file(path: Path) -> Iterable[Tuple[str, str]]:
    for raw in path.read_text().splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        yield key.strip().lower(), value.strip()


def _parse_toml_file(path: Path) -> dict[str, str]:
    data = tomllib.loads(path.read_text())
    section = data.get("caldav")
    if not isinstance(section, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in section.items():
        if value is None:
            continue
        result[key.lower()] = str(value).strip()
    return result


def _load_file_values(path: Path) -> dict[str, str]:
    if path.suffix == ".toml":
        return _parse_toml_file(path)
    return dict(_parse_config_file(path))


def load_config(env: str | None = None, config_home: Path | None = None) -> CaldavConfig:
    values = {
        "calendar_url": os.environ.get("TODO_CALDAV_URL"),
        "username": os.environ.get("TODO_USERNAME"),
        "password": os.environ.get("TODO_PASSWORD"),
        "token": os.environ.get("TODO_TOKEN"),
    }
    path = config_file_path(env, config_home)
    if not path.exists():
        legacy = path.with_suffix("")
        if legacy.exists():
            path = legacy
    if path.exists():
        file_values = _load_file_values(path)
        values.update(file_values)
    url = values.get("calendar_url")
    username = values.get("username")
    password = values.get("password")
    token = values.get("token")
    if not url or not username:
        raise RuntimeError("caldav configuration requires calendar_url and username")
    return CaldavConfig(calendar_url=url, username=username, password=password, token=token)
