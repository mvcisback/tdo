from __future__ import annotations

from dataclasses import dataclass, field
import argparse
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
from .config import (
    CaldavConfig,
    config_file_path,
    load_config,
    load_config_from_path,
    write_config_file,
)
from .models import Task, TaskPatch, TaskPayload
from .time_parser import parse_due_value, parse_wait_value
from .update_parser import UpdateDescriptor, parse_update


T = TypeVar("T")
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


_COMMAND_NAMES = {"add", "config", "del", "do", "list", "modify"}


def _split_filter_and_command(argv: Sequence[str]) -> tuple[str | None, list[str]]:
    candidates = list(argv)
    if not candidates:
        return None, ["list"]
    first, rest = candidates[0], candidates[1:]
    if first in _COMMAND_NAMES:
        return None, candidates
    if not rest:
        return first, ["list"]
    return first, rest


def _parse_filter_indices(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    tokens = [segment.strip() for segment in raw.split(",")]
    normalized = [token for token in tokens if token]
    for token in normalized:
        if not token.isdigit():
            typer.echo(f"invalid filter token: {token}")
            raise typer.Exit(code=1)
    return normalized


def _effective_filter_indices(indices: list[str] | None) -> list[str]:
    if indices is None:
        return []
    return indices


def _select_tasks_for_filter(tasks: list[Task], indices: list[str]) -> list[Task]:
    if not tasks:
        return []
    sorted_tasks = sorted(tasks, key=_task_sort_key)
    if not indices:
        return list(sorted_tasks)
    index_map = {str(index + 1): task for index, task in enumerate(sorted_tasks)}
    selected: list[Task] = []
    for token in indices:
        task = index_map.get(token)
        if task is None:
            typer.echo(f"filter {token} did not match any task")
            raise typer.Exit(code=1)
        selected.append(task)
    return selected


def _normalize_tokens(tokens: Sequence[str] | None) -> list[str]:
    return [token for token in tokens or [] if token != "--"]


def _handle_add(args: argparse.Namespace) -> None:
    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    metadata = _parse_metadata(tokens)
    payload = _build_payload(args.description, descriptor, metadata)
    created = _run_with_client(args.env, lambda client: client.create_task(payload))
    typer.echo(created.uid)


def _handle_modify(args: argparse.Namespace) -> None:
    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    metadata = _parse_metadata(tokens)
    if not _has_update_candidates(descriptor, metadata):
        typer.echo("no changes provided")
        raise typer.Exit(code=1)

    def callback(client: CalDAVClient) -> int:
        tasks = _select_tasks_for_filter(
            _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            typer.echo("no tasks match filter")
            raise typer.Exit(code=1)
        modified = 0
        for task in tasks:
            patch = _build_patch_from_descriptor(descriptor, metadata, task)
            if not patch.has_changes():
                continue
            client.modify_task(task.uid, patch)
            modified += 1
        if modified == 0:
            typer.echo("no changes provided")
            raise typer.Exit(code=1)
        return modified

    modified_count = _run_with_client(args.env, callback)
    typer.echo(f"modified {modified_count} tasks")


def _handle_do(args: argparse.Namespace) -> None:
    patch = TaskPatch(status="COMPLETED")

    def mark_done_many(client: CalDAVClient) -> list[str]:
        tasks = _select_tasks_for_filter(
            _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            typer.echo("no tasks match filter")
            raise typer.Exit(code=1)
        completed: list[str] = []
        for task in tasks:
            client.modify_task(task.uid, patch)
            completed.append(task.uid)
        return completed

    completed = _run_with_client(args.env, mark_done_many)
    typer.echo(f"marked {len(completed)} tasks as done")


def _handle_delete(args: argparse.Namespace) -> None:
    def callback(client: CalDAVClient) -> list[str]:
        tasks = _select_tasks_for_filter(
            _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            typer.echo("no tasks match filter")
            raise typer.Exit(code=1)
        return _delete_many(client, [task.uid for task in tasks])

    deleted = _run_with_client(args.env, callback)
    typer.echo(f"deleted {len(deleted)} tasks")


def _handle_list(args: argparse.Namespace) -> None:
    config = _resolve_config(args.env)
    tasks = _run_with_client(args.env, lambda client: client.list_tasks())
    if not tasks:
        typer.echo("no tasks found")
        return
    filtered_tasks = _select_tasks_for_filter(
        tasks,
        _effective_filter_indices(args.filter_indices),
    )
    if not filtered_tasks:
        typer.echo("no tasks match filter")
        return
    _pretty_print_tasks(filtered_tasks, config.show_uids)


def _handle_config_init(args: argparse.Namespace) -> None:
    target = config_file_path(args.env, args.config_home)
    calendar_url_value = _require_value(args.calendar_url, "CalDAV calendar URL")
    username_value = _require_value(args.username, "CalDAV username")
    password_value = args.password if args.password else None
    token_value = args.token if args.token else None
    config = CaldavConfig(
        calendar_url=calendar_url_value,
        username=username_value,
        password=password_value,
        token=token_value,
    )
    try:
        path = write_config_file(target, config, force=args.force)
    except FileExistsError:
        typer.echo(f"{target} already exists; use --force to overwrite")
        raise typer.Exit(code=1)
    typer.echo(f"created config file at {path}")


def _handle_config_help(args: argparse.Namespace) -> None:
    parser = getattr(args, "parser", None)
    if parser:
        parser.print_help()
    else:
        typer.echo("config command requires a subcommand")
        raise typer.Exit(code=1)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="todo")
    subparsers = parser.add_subparsers(dest="command")

    add_parser = subparsers.add_parser("add")
    add_parser.add_argument("--env", dest="env", help="env name")
    add_parser.add_argument("description", help="task description")
    add_parser.add_argument("tokens", nargs=argparse.REMAINDER, default=[], help="taskwarrior tokens")
    add_parser.set_defaults(func=_handle_add)

    modify_parser = subparsers.add_parser("modify")
    modify_parser.add_argument("--env", dest="env", help="env name")
    modify_parser.add_argument("tokens", nargs=argparse.REMAINDER, default=[], help="taskwarrior tokens")
    modify_parser.set_defaults(func=_handle_modify)

    do_parser = subparsers.add_parser("do")
    do_parser.add_argument("--env", dest="env", help="env name")
    do_parser.set_defaults(func=_handle_do)

    delete_parser = subparsers.add_parser("del")
    delete_parser.add_argument("--env", dest="env", help="env name")
    delete_parser.set_defaults(func=_handle_delete)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--env", dest="env", help="env name")
    list_parser.set_defaults(func=_handle_list)

    config_parser = subparsers.add_parser("config")
    config_parser.set_defaults(func=_handle_config_help, parser=config_parser)
    config_subparsers = config_parser.add_subparsers(dest="subcommand")
    init_parser = config_subparsers.add_parser("init")
    init_parser.add_argument("--env", dest="env", help="environment name")
    init_parser.add_argument(
        "--config-home",
        dest="config_home",
        type=Path,
        default=None,
        help="override the config directory",
    )
    init_parser.add_argument("--calendar-url", dest="calendar_url", help="CalDAV calendar URL")
    init_parser.add_argument("--username", dest="username", help="CalDAV username")
    init_parser.add_argument("--password", dest="password", help="CalDAV password")
    init_parser.add_argument("--token", dest="token", help="CalDAV token")
    init_parser.add_argument("--force", dest="force", action="store_true", help="overwrite existing config")
    init_parser.set_defaults(func=_handle_config_init, parser=config_parser)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    input_args = list(argv if argv is not None else sys.argv[1:])
    filter_raw, command_tokens = _split_filter_and_command(input_args)
    parser = _build_parser()
    args = parser.parse_args(command_tokens)
    args.filter_indices = _parse_filter_indices(filter_raw)
    handler = getattr(args, "func", None)
    if handler is None:
        parser.print_help()
        return 0
    handler(args)
    return 0
