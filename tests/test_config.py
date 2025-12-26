from __future__ import annotations

import pytest
from pathlib import Path

from todo.config import CaldavConfig, config_file_path, load_config, write_config_file


CONFIG = CaldavConfig(
    calendar_url="https://example.com/cal",
    username="tester",
    password="secret",
    token="tok",
)


def test_config_file_path_uses_env(tmp_path: Path) -> None:
    target = config_file_path("stage", config_home=tmp_path)
    assert target == tmp_path / "config.stage.toml"


def test_write_config_file_persists_values(tmp_path: Path) -> None:
    target = tmp_path / "config.local"
    result = write_config_file(target, CONFIG)
    assert result == target
    text = target.read_text()
    assert text.startswith("[caldav]")
    assert "calendar_url = \"https://example.com/cal\"" in text
    assert "username = \"tester\"" in text
    assert "password = \"secret\"" in text
    assert "token = \"tok\"" in text


def test_write_config_file_requires_force(tmp_path: Path) -> None:
    target = tmp_path / "config.local"
    write_config_file(target, CONFIG)
    with pytest.raises(FileExistsError):
        write_config_file(target, CONFIG)
    # force overwrites
    write_config_file(target, CONFIG, force=True)


def test_load_config_reads_written_file(tmp_path: Path) -> None:
    home = tmp_path
    target = config_file_path("app", config_home=home)
    write_config_file(target, CaldavConfig(calendar_url="https://example.com", username="alice"), force=True)
    loaded = load_config(env="app", config_home=home)
    assert loaded.username == "alice"
    assert loaded.calendar_url == "https://example.com"


def test_load_config_supports_legacy_format(tmp_path: Path) -> None:
    home = tmp_path
    legacy = home / "config.legacy"
    legacy.write_text("calendar_url = https://example.com\nusername = alice\n")
    loaded = load_config(env="legacy", config_home=home)
    assert loaded.username == "alice"
    assert loaded.calendar_url == "https://example.com"
