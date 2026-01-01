from __future__ import annotations

import getpass
import json
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Tuple, Union

import keyring

DEFAULT_TRANSACTION_LOG_SIZE = 32


@dataclass
class CacheConfig:
    transaction_log_size: int = DEFAULT_TRANSACTION_LOG_SIZE


@dataclass
class CaldavConfig:
    calendar_url: str
    username: str
    password: str | None = None
    token: str | None = None
    env: str = "default"
    show_uids: bool = False
    cache: CacheConfig = field(default_factory=CacheConfig)

    @property
    def keyring_service(self) -> str:
        return f"tdo-{self.env}"

    def getpass(self):
        password = self.password
        if password is None:
            password = _retrieve_password_from_keyring(self.keyring_service, self.username)
        if password is None:
            password = getpass.getpass()
            keyring.set_password(self.keyring_service, self.username, password)
        return password



def resolve_env(env: str | None = None) -> str:
    if env is not None:
        return env
    return os.environ.get("TDO_ENV", "default")


def config_file_path(env: str | None = None, config_home: Path | None = None) -> Path:
    base_home = config_home or Path.home() / ".config" / "tdo"
    resolved_env = resolve_env(env)
    return base_home / f"config.{resolved_env}.toml"


def write_config_file(path: Path, config: CaldavConfig, *, force: bool = False) -> Path:
    # TODO: Use built in dataclass -> dict for this.
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
    if config.env:
        lines.append(f"env = {json.dumps(config.env)}")

    # Write cache section if non-default
    if config.cache.transaction_log_size != DEFAULT_TRANSACTION_LOG_SIZE:
        lines.append("")
        lines.append("[cache]")
        lines.append(f"transaction_log_size = {config.cache.transaction_log_size}")

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


def _parse_toml_file(path: Path) -> dict[str, Union[str, bool, int]]:
    data = tomllib.loads(path.read_text())
    result: dict[str, Union[str, bool, int]] = {}

    # Parse [caldav] section
    caldav_section = data.get("caldav")
    if isinstance(caldav_section, dict):
        for key, value in caldav_section.items():
            if value is None:
                continue
            result[key.lower()] = value

    # Parse [cache] section
    cache_section = data.get("cache")
    if isinstance(cache_section, dict):
        for key, value in cache_section.items():
            if value is None:
                continue
            result[f"cache.{key.lower()}"] = value

    return result


def _load_file_values(path: Path) -> dict[str, Union[str, bool, int]]:
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


def _parse_int_like(value: str | int | None, default: int) -> int:
    """Parse an integer value with fallback to default."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except ValueError:
        return default


def load_config(env: str | None = None, config_home: Path | None = None) -> CaldavConfig:
    resolved_env = resolve_env(env)
    values: dict[str, str | bool | int | None] = {
        "calendar_url": os.environ.get("TDO_CALDAV_URL"),
        "username": os.environ.get("TDO_USERNAME"),
        "password": os.environ.get("TDO_PASSWORD"),
        "token": os.environ.get("TDO_TOKEN"),
        "show_uids": os.environ.get("TDO_SHOW_UIDS"),
        "cache.transaction_log_size": os.environ.get("TDO_TRANSACTION_LOG_SIZE"),
    }

    path = config_file_path(resolved_env, config_home)
    if not path.exists():
        legacy = path.with_suffix("")
        if legacy.exists():
            path = legacy
    if path.exists():
        file_values = _load_file_values(path)
        values.update(file_values)
    return _build_config(values, resolved_env)


def load_config_from_path(path: Path, env: str | None = None) -> CaldavConfig:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    values = _load_file_values(path)
    resolved_env = resolve_env(env)
    return _build_config(values, resolved_env)


def _build_config(
    values: dict[str, str | bool | int | None], resolved_env: str
) -> CaldavConfig:
    url = values.get("calendar_url")
    username = values.get("username")
    password = values.get("password")
    token = values.get("token")
    show_uids = _parse_bool_like(values.get("show_uids"))

    # Build cache config
    transaction_log_size = _parse_int_like(
        values.get("cache.transaction_log_size"), DEFAULT_TRANSACTION_LOG_SIZE
    )
    cache_config = CacheConfig(transaction_log_size=transaction_log_size)

    if not url or not username:
        raise RuntimeError("caldav configuration requires calendar_url and username")

    return CaldavConfig(
        calendar_url=url,
        username=username,
        password=password,
        token=token,
        env=resolved_env,
        show_uids=show_uids if show_uids is not None else False,
        cache=cache_config,
    )
