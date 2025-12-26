from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Sequence, TypeVar

import typer
from rich import box
from rich.console import Console
from rich.table import Table

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


def _sorted_tasks(client: CalDAVClient) -> list[Task]:
    return sorted(client.list_tasks(), key=_task_sort_key)


def _resolve_task_identifier(client: CalDAVClient, identifier: str) -> str:
    tasks = _sorted_tasks(client)
    index_map = {str(index + 1): task.uid for index, task in enumerate(tasks)}
    candidate = identifier.strip()
    if not candidate:
        return identifier
    return index_map.get(candidate, identifier)


def _resolve_delete_targets(client: CalDAVClient, targets: list[str]) -> list[str]:
    return [_resolve_task_identifier(client, token) for token in targets]


def _task_sort_key(task: Task) -> tuple[datetime, int, str]:
    due_key = task.due or datetime.max
    priority_key = task.priority if task.priority is not None else 10
    summary_key = task.summary.strip().lower() if task.summary else ""
    return due_key, priority_key, summary_key


def _format_due_label(due: datetime | None, now: datetime) -> str:
    if due is None:
        return "--"
    delta = due - now
    sign = "-" if delta.total_seconds() < 0 else ""
    delta = abs(delta)
    if delta.days > 0:
        return f"{sign}{delta.days}d"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"{sign}{hours}h"
    minutes = (delta.seconds % 3600) // 60
    if minutes > 0:
        return f"{sign}{minutes}m"
    return f"{sign}0m"


SUMMARY_WIDTH = 45


def _format_project(task: Task) -> str:
    project = task.x_properties.get("X-PROJECT") or task.x_properties.get("X-TASKS-ORG-ORDER")
    return project or "-"


def _format_tag(task: Task) -> str:
    tag = task.x_properties.get("X-TAG") or task.x_properties.get("X-COLOR")
    return tag or "-"


def _format_due_date(due: datetime | None) -> str:
    if not due:
        return "-"
    return due.strftime("%Y-%m-%d")


def _pretty_print_tasks(tasks: list[Task], show_uids: bool) -> None:
    console = Console(file=sys.stdout, color_system="auto")
    table = Table(
        box=box.SIMPLE_HEAVY,
        show_header=True,
        header_style="bold cyan",
        row_styles=["", "dim"],
        padding=(0, 1),
    )
    table.add_column("ID", justify="right", style="cyan", width=3)
    table.add_column("Age", justify="right", style="bright_blue", width=4)
    table.add_column("Project", style="magenta", width=12)
    table.add_column("Tag", style="yellow", width=10)
    table.add_column("Due", style="bright_green", width=10)
    table.add_column("Description", style="white", width=45)
    table.add_column("Urg", justify="right", style="bright_red", width=4)
    if show_uids:
        table.add_column("UID", style="dim")
    now = datetime.now()
    sorted_tasks = sorted(tasks, key=_task_sort_key)
    for index, task in enumerate(sorted_tasks, start=1):
        due_label = _format_due_label(task.due, now)
        project = _format_project(task)
        tag = _format_tag(task)
        due_date = _format_due_date(task.due)
        summary = task.summary or ""
        trimmed_summary = (
            summary[: SUMMARY_WIDTH - 3] + "..."
            if len(summary) > SUMMARY_WIDTH
            else summary
        )
        priority_label = str(task.priority) if task.priority is not None else "-"
        row = [
            str(index),
            due_label,
            project,
            tag,
            due_date,
            trimmed_summary,
            priority_label,
        ]
        if show_uids:
            row.append(task.uid)
        table.add_row(*row)
    console.print(table)


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

    def callback(client: CalDAVClient) -> Task:
        target_uid = _resolve_task_identifier(client, uid)
        return client.modify_task(target_uid, patch)

    updated = _run_with_client(env, config_file, callback)
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
    deleted = _run_with_client(
        env,
        config_file,
        lambda client: _delete_many(client, _resolve_delete_targets(client, targets)),
    )
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
    config = _resolve_config(env, config_file)
    tasks = _run_with_client(env, config_file, lambda client: client.list_tasks())
    if not tasks:
        typer.echo("no tasks found")
        return
    _pretty_print_tasks(tasks, config.show_uids)


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
