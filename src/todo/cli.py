from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Sequence, TypeVar

import typer

from .caldav_client import CalDAVClient
from .config import CaldavConfig, config_file_path, load_config, load_config_from_path, write_config_file
from .models import TaskPatch, TaskPayload


T = TypeVar("T")
app = typer.Typer()
config_app = typer.Typer(help="Manage CalDAV configuration.")
app.add_typer(config_app, name="config")
_CLIENT_FACTORY: type[CalDAVClient] = CalDAVClient


def _run_with_client(env: str | None, config_file: Path | None, callback: Callable[[CalDAVClient], T]) -> T:
    config = _resolve_config(env, config_file)
    with _CLIENT_FACTORY(config) as client:
        return callback(client)


def _resolve_config(env: str | None, config_file: Path | None) -> CaldavConfig:
    if config_file:
        return load_config_from_path(config_file)
    return load_config(env)


def _require_value(value: str | None, prompt_text: str) -> str:
    candidate = (value or "").strip()
    if candidate:
        return candidate
    response = typer.prompt(prompt_text).strip()
    if not response:
        typer.echo(f"{prompt_text} is required")
        raise typer.Exit(code=1)
    return response


def _parse_priority(raw: str) -> int | None:
    candidate = raw.strip().lower()
    if candidate in {"h", "high"}:
        return 1
    if candidate in {"m", "medium"}:
        return 5
    if candidate in {"l", "low"}:
        return 9
    try:
        value = int(raw)
        return value
    except ValueError:
        return None


def _parse_due(raw: str) -> datetime | None:
    lowered = raw.strip().lower()
    now = datetime.now()
    if lowered == "today":
        return now.replace(hour=23, minute=59, second=59, microsecond=0)
    if lowered == "tomorrow":
        target = now + timedelta(days=1)
        return target.replace(hour=23, minute=59, second=59, microsecond=0)
    if lowered == "eod":
        return now.replace(hour=23, minute=59, second=59, microsecond=0)
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _parse_tokens(tokens: Sequence[str]) -> tuple[int | None, datetime | None, dict[str, str]]:
    priority: int | None = None
    due: datetime | None = None
    x_properties: dict[str, str] = {}
    for token in tokens:
        parts = token.split(":", 2)
        if not parts:
            continue
        key = parts[0].strip().lower()
        if key == "pri" and len(parts) > 1:
            parsed = _parse_priority(parts[1])
            if parsed is not None:
                priority = parsed
        elif key == "due" and len(parts) > 1:
            parsed = _parse_due(parts[1])
            if parsed is not None:
                due = parsed
        elif key == "x" and len(parts) == 3:
            x_properties[parts[1]] = parts[2]
    return priority, due, x_properties


def _parse_payload(description: str, tokens: Sequence[str]) -> TaskPayload:
    priority, due, x_properties = _parse_tokens(tokens)
    return TaskPayload(summary=description, priority=priority, due=due, x_properties=x_properties)


def _parse_patch(tokens: Sequence[str]) -> TaskPatch:
    priority, due, x_properties = _parse_tokens(tokens)
    summary: str | None = None
    for token in tokens:
        parts = token.split(":", 1)
        if len(parts) < 2:
            continue
        if parts[0].strip().lower() == "summary":
            summary = parts[1]
    return TaskPatch(summary=summary, priority=priority, due=due, x_properties=x_properties)


def _delete_many(client: CalDAVClient, targets: list[str]) -> list[str]:
    deleted: list[str] = []
    for uid in targets:
        client.delete_task(uid)
        deleted.append(uid)
    return deleted


@app.command()
def add(
    description: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    config_file: Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to an existing CalDAV config TOML.",
    ),
    tokens: list[str] | None = typer.Argument(None),
):
    payload = _parse_payload(description, tokens or ())
    created = _run_with_client(env, config_file, lambda client: client.create_task(payload))
    typer.echo(created.uid)


@app.command()
def modify(
    uid: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    config_file: Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to an existing CalDAV config TOML.",
    ),
    tokens: list[str] | None = typer.Argument(None),
):
    patch = _parse_patch(tokens or ())
    if not patch.has_changes():
        typer.echo("no changes provided")
        raise typer.Exit(code=1)
    updated = _run_with_client(env, config_file, lambda client: client.modify_task(uid, patch))
    typer.echo(updated.uid)


@app.command(name="del")
def delete(
    uids: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    config_file: Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to an existing CalDAV config TOML.",
    ),
):
    targets = [token.strip() for token in uids.split(",") if token.strip()]
    if not targets:
        typer.echo("no targets provided")
        raise typer.Exit(code=1)
    deleted = _run_with_client(env, config_file, lambda client: _delete_many(client, targets))
    typer.echo(f"deleted {len(deleted)} tasks")


@app.command(name="list")
def list_tasks(
    env: str | None = typer.Option(None, "--env", help="env name"),
    config_file: Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to an existing CalDAV config TOML.",
    ),
) -> None:
    tasks = _run_with_client(env, config_file, lambda client: client.list_tasks())
    if not tasks:
        typer.echo("no tasks found")
        return
    for task in tasks:
        due = task.due.isoformat() if task.due else "-"
        typer.echo(f"{task.uid}\t{due}\t{task.summary}")


@config_app.command(name="init")
def init_config(
    env: str | None = typer.Option(None, "--env", help="environment name"),
    config_home: Path | None = typer.Option(
        None,
        "--config-home",
        help="override the config directory",
    ),
    calendar_url: str | None = typer.Option(None, "--calendar-url", help="CalDAV calendar URL"),
    username: str | None = typer.Option(None, "--username", help="CalDAV username"),
    password: str | None = typer.Option(None, "--password", help="CalDAV password"),
    token: str | None = typer.Option(None, "--token", help="CalDAV token"),
    force: bool = typer.Option(False, "--force", "-f", help="overwrite existing config"),
) -> None:
    target = config_file_path(env, config_home)
    calendar_url_value = _require_value(calendar_url, "CalDAV calendar URL")
    username_value = _require_value(username, "CalDAV username")
    password_value = password if password else None
    token_value = token if token else None
    config = CaldavConfig(
        calendar_url=calendar_url_value,
        username=username_value,
        password=password_value,
        token=token_value,
    )
    try:
        path = write_config_file(target, config, force=force)
    except FileExistsError:
        typer.echo(f"{target} already exists; use --force to overwrite")
        raise typer.Exit(code=1)
    typer.echo(f"created config file at {path}")
