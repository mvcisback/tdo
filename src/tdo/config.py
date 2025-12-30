from __future__ import annotations

import getpass
import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple, Union

import keyring


@dataclass
class CaldavConfig:
    calendar_url: str
    username: str
    password: str | None = None
    token: str | None = None
    keyring_service: str = "tdo"
    show_uids: bool = False

    def getpass(self):
        password = self.password
        if password is None:
            password = _retrieve_password_from_keyring(self.keyring_service, self.username)
        if password is None:
            password = getpass.getpass()
            keyring.set_password(self.keyring_service, self.username, password)
        return password



def resolve_env(env: str | None = None) -> str:
    return env or os.environ.get("TDO_ENV") or "default"


def config_file_path(env: str | None = None, config_home: Path | None = None) -> Path:
    base_home = config_home or Path.home() / ".config" / "tdo"
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
    if config.keyring_service:
        lines.append(f"keyring_service = {json.dumps(config.keyring_service)}")
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


def _parse_toml_file(path: Path) -> dict[str, Union[str, bool]]:
    data = tomllib.loads(path.read_text())
    section = data.get("caldav")
    if not isinstance(section, dict):
        return {}
    result: dict[str, Union[str, bool]] = {}
    for key, value in section.items():
        if value is None:
            continue
        result[key.lower()] = value
    return result


def _load_file_values(path: Path) -> dict[str, Union[str, bool]]:
    if path.suffix == ".toml":
        return _parse_toml_file(path)
    return dict(_parse_config_file(path))


def _retrieve_password_from_keyring(service: str, username: str) -> str | None:
    try:
        import keyring
    except ModuleNotFoundError:
        return None
    try:
        return keyring.get_password(service, username)
    except Exception:
        return None


def _parse_bool_like(value: str | bool | None) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    candidate = str(value).strip().lower()
    if candidate in {"true", "1", "yes", "y", "on"}:
        return True
    if candidate in {"false", "0", "no", "n", "off"}:
        return False
    return None


def load_config(env: str | None = None, config_home: Path | None = None) -> CaldavConfig:
    values = {
        "calendar_url": os.environ.get("TDO_CALDAV_URL"),
        "username": os.environ.get("TDO_USERNAME"),
        "password": os.environ.get("TDO_PASSWORD"),
        "token": os.environ.get("TDO_TOKEN"),
        "keyring_service": os.environ.get("TDO_KEYRING_SERVICE", "tdo"),
        "show_uids": os.environ.get("TDO_SHOW_UIDS"),
    }

    path = config_file_path(env, config_home)
    if not path.exists():
        legacy = path.with_suffix("")
        if legacy.exists():
            path = legacy
    if path.exists():
        file_values = _load_file_values(path)
        values.update(file_values)
    return _build_config(values)


def load_config_from_path(path: Path) -> CaldavConfig:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    values = _load_file_values(path)
    return _build_config(values)


def _build_config(values: dict[str, str | bool | None]) -> CaldavConfig:
    url = values.get("calendar_url")
    username = values.get("username")
    password = values.get("password")
    token = values.get("token")
    keyring_service = values.get("keyring_service", "tdo")
    show_uids = _parse_bool_like(values.get("show_uids"))
    if not url or not username:
        raise RuntimeError("caldav configuration requires calendar_url and username")
         
    return CaldavConfig(
        calendar_url=url,
        username=username,
        password=password,
        token=token,
        keyring_service=keyring_service,
        show_uids=show_uids if show_uids is not None else False,
    )
