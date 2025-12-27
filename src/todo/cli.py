from __future__ import annotations

from dataclasses import dataclass, field
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Sequence, TypeVar

import typer
from parsimonious.exceptions import ParseError
from rich import box
from rich.console import Console
from rich.table import Table

from .caldav_client import CalDAVClient
from .config import CaldavConfig, config_file_path, load_config, load_config_from_path, write_config_file
from .models import Task, TaskPatch, TaskPayload
from .time_parser import parse_due_value, parse_wait_value
from .update_parser import UpdateDescriptor, parse_update


T = TypeVar("T")
app = typer.Typer()
config_app = typer.Typer(help="Manage CalDAV configuration.")
app.add_typer(config_app, name="config")
_CLIENT_FACTORY: type[CalDAVClient] = CalDAVClient


def _run_with_client(env: str | None, callback: Callable[[CalDAVClient], T]) -> T:
    config = _resolve_config(env)
    with _CLIENT_FACTORY(config) as client:
        return callback(client)


def _resolve_config(env: str | None) -> CaldavConfig:
    config_path = os.environ.get("TODO_CONFIG_FILE")
    if config_path:
        return load_config_from_path(Path(config_path).expanduser())
    return load_config(env)


@dataclass
class UpdateMetadata:
    priority: int | None = None
    status: str | None = None
    summary: str | None = None
    x_properties: dict[str, str] = field(default_factory=dict)


def _merge_tags(existing: str | None, additions: Sequence[str]) -> str | None:
    normalized: list[str] = []
    if existing:
        normalized.extend(tag.strip() for tag in existing.split(",") if tag.strip())
    for addition in additions:
        candidate = addition.strip()
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return ",".join(normalized) if normalized else None


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


def _parse_metadata(tokens: Sequence[str]) -> UpdateMetadata:
    metadata = UpdateMetadata()
    for token in tokens:
        candidate = token.strip()
        if not candidate or ":" not in candidate:
            continue
        key, rest = candidate.split(":", 1)
        key_lower = key.strip().lower()
        value = rest.strip()
        if key_lower == "pri":
            priority = _parse_priority(value)
            if priority is not None:
                metadata.priority = priority
            continue
        if key_lower == "status":
            metadata.status = value.upper() if value else None
            continue
        if key_lower == "summary":
            metadata.summary = rest
            continue
        if key_lower == "x" and ":" in rest:
            prop_key, prop_value = rest.split(":", 1)
            metadata.x_properties[prop_key] = prop_value
    return metadata


def _parse_update_descriptor(tokens: Sequence[str]) -> UpdateDescriptor:
    raw = " ".join(token.strip() for token in tokens if token and token.strip())
    try:
        return parse_update(raw)
    except ParseError as exc:
        typer.echo(f"unable to parse update arguments: {exc}")
        raise typer.Exit(code=1)


def _resolve_due_value(raw: str | None) -> datetime | None:
    if not raw:
        return None
    resolved = parse_due_value(raw)
    if resolved is None:
        return None
    return resolved.to("UTC").naive


def _apply_tag_changes(existing: str | None, descriptor: UpdateDescriptor) -> str | None:
    if not descriptor.add_tags and not descriptor.remove_tags:
        return None
    normalized = {tag.strip() for tag in (existing or "").split(",") if tag.strip()}
    normalized.update(descriptor.add_tags)
    normalized.difference_update(descriptor.remove_tags)
    return ",".join(sorted(normalized))


def _has_update_candidates(descriptor: UpdateDescriptor, metadata: UpdateMetadata) -> bool:
    return bool(
        metadata.summary
        or metadata.priority is not None
        or metadata.status
        or metadata.x_properties
        or descriptor.project
        or descriptor.due
        or descriptor.wait
        or descriptor.add_tags
        or descriptor.remove_tags
    )


def _build_payload(description: str, descriptor: UpdateDescriptor, metadata: UpdateMetadata) -> TaskPayload:
    summary = metadata.summary or description
    due = _resolve_due_value(descriptor.due)
    x_properties = dict(metadata.x_properties)
    if descriptor.project:
        x_properties["X-PROJECT"] = descriptor.project
    if descriptor.add_tags:
        merged_tags = _merge_tags(x_properties.get("X-TAGS"), tuple(descriptor.add_tags))
        if merged_tags:
            x_properties["X-TAGS"] = merged_tags
    if descriptor.wait:
        wait_value = descriptor.wait.strip()
        if wait_value:
            x_properties["X-WAIT"] = wait_value
    return TaskPayload(
        summary=summary,
        priority=metadata.priority,
        due=due,
        status=metadata.status or "IN-PROCESS",
        x_properties=x_properties,
    )


def _build_patch_from_descriptor(
    descriptor: UpdateDescriptor, metadata: UpdateMetadata, existing: Task | None
) -> TaskPatch:
    due = _resolve_due_value(descriptor.due)
    patch = TaskPatch(
        summary=metadata.summary,
        priority=metadata.priority,
        due=due,
        status=metadata.status,
    )
    x_properties = dict(metadata.x_properties)
    if descriptor.project:
        x_properties["X-PROJECT"] = descriptor.project
    if descriptor.wait:
        wait_value = descriptor.wait.strip()
        if wait_value and parse_wait_value(wait_value) is not None:
            x_properties["X-WAIT"] = wait_value
    tags_value = _apply_tag_changes(existing.x_properties.get("X-TAGS") if existing else None, descriptor)
    if tags_value is not None:
        if tags_value:
            x_properties["X-TAGS"] = tags_value
        else:
            x_properties.pop("X-TAGS", None)
    patch.x_properties = x_properties
    return patch


def _delete_many(client: CalDAVClient, targets: list[str]) -> list[str]:
    deleted: list[str] = []
    for uid in targets:
        client.delete_task(uid)
        deleted.append(uid)
    return deleted


def _sorted_tasks(client: CalDAVClient) -> list[Task]:
    return sorted(client.list_tasks(), key=_task_sort_key)


def _resolve_task_identifier(
    client: CalDAVClient, identifier: str, tasks: list[Task] | None = None
) -> str:
    source_tasks = tasks or _sorted_tasks(client)
    index_map = {str(index + 1): task.uid for index, task in enumerate(source_tasks)}
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
    tags = task.x_properties.get("X-TAGS")
    if tags:
        return tags
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
    tokens: list[str] | None = typer.Argument(None),
):
    descriptor = _parse_update_descriptor(tokens or ())
    metadata = _parse_metadata(tokens or ())
    payload = _build_payload(description, descriptor, metadata)
    created = _run_with_client(env, lambda client: client.create_task(payload))
    typer.echo(created.uid)


@app.command()
def modify(
    uid: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    tokens: list[str] | None = typer.Argument(None),
):
    descriptor = _parse_update_descriptor(tokens or ())
    metadata = _parse_metadata(tokens or ())
    if not _has_update_candidates(descriptor, metadata):
        typer.echo("no changes provided")
        raise typer.Exit(code=1)

    def callback(client: CalDAVClient) -> Task:
        tasks = _sorted_tasks(client)
        target_uid = _resolve_task_identifier(client, uid, tasks)
        existing = next((task for task in tasks if task.uid == target_uid), None)
        if existing is None:
            existing = Task(uid=target_uid, summary=target_uid, due=None, priority=None)
        patch = _build_patch_from_descriptor(descriptor, metadata, existing)
        if not patch.has_changes():
            typer.echo("no changes provided")
            raise typer.Exit(code=1)
        return client.modify_task(target_uid, patch)

    updated = _run_with_client(env, callback)
    typer.echo(updated.uid)


@app.command()
def do(
    uids: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
):
    targets = [token.strip() for token in uids.split(",") if token.strip()]
    if not targets:
        typer.echo("no targets provided")
        raise typer.Exit(code=1)
    
    patch = TaskPatch(status="COMPLETED")
    
    def mark_done_many(client: CalDAVClient) -> list[str]:
        completed: list[str] = []
        for target in targets:
            resolved_uid = _resolve_task_identifier(client, target)
            client.modify_task(resolved_uid, patch)
            completed.append(resolved_uid)
        return completed
    
    completed = _run_with_client(env, mark_done_many)
    typer.echo(f"marked {len(completed)} tasks as done")


@app.command(name="del")
def delete(
    uids: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
):
    targets = [token.strip() for token in uids.split(",") if token.strip()]
    if not targets:
        typer.echo("no targets provided")
        raise typer.Exit(code=1)
    deleted = _run_with_client(
        env,
        lambda client: _delete_many(client, _resolve_delete_targets(client, targets)),
    )
    typer.echo(f"deleted {len(deleted)} tasks")


@app.command(name="list")
def list_tasks(
    env: str | None = typer.Option(None, "--env", help="env name"),
) -> None:
    config = _resolve_config(env)
    tasks = _run_with_client(env, lambda client: client.list_tasks())
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
