from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Awaitable, Callable, NoReturn, Sequence, TypeVar

from rich import box
from rich.console import Console
from rich.table import Table

from .config import (
    CaldavConfig,
    config_file_path,
    load_config,
    load_config_from_path,
    write_config_file,
)
from .models import Task, TaskPatch, TaskPayload
from .time_parser import parse_due_value
from .update_descriptor import UpdateDescriptor
from .update_linear_parser import parse_update


T = TypeVar("T")

# Sentinel value to indicate a datetime field should be explicitly unset
_UNSET_DATETIME = datetime(1, 1, 1, 0, 0, 0)


def _get_version() -> str:
    try:
        return version("tdo")
    except PackageNotFoundError:
        return "dev"


async def _run_with_client(env: str | None, callback: Callable[["CalDAVClient"], Awaitable[T]]) -> T:
    from .caldav_client import CalDAVClient

    config = _resolve_config(env)
    client = await CalDAVClient.create(config)
    try:
        with client:
            return await callback(client)
    finally:
        await client.close()


async def _cache_client(env: str | None) -> "CalDAVClient":
    from .caldav_client import CalDAVClient

    config = _resolve_config(env)
    return await CalDAVClient.create(config)


def _resolve_config(env: str | None) -> CaldavConfig:
    config_path = os.environ.get("TDO_CONFIG_FILE")
    if config_path:
        return load_config_from_path(Path(config_path).expanduser(), env=env)
    return load_config(env)




def _split_categories_value(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [segment.strip() for segment in raw.split(",") if segment.strip()]


def _exit_with_message(message: str) -> NoReturn:
    print(message)
    raise SystemExit(1)


def _require_value(value: str | None, prompt_text: str) -> str:
    candidate = (value or "").strip()
    if candidate:
        return candidate
    response = input(prompt_text).strip()
    if not response:
        _exit_with_message(f"{prompt_text} is required")
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




def _parse_update_descriptor(tokens: Sequence[str]) -> UpdateDescriptor:
    raw = " ".join(token.strip() for token in tokens if token and token.strip())
    return parse_update(raw)


def _resolve_due_value(raw: str | None) -> datetime | None:
    if not raw:
        return None
    resolved = parse_due_value(raw)
    if resolved is None:
        return None
    return resolved.to("UTC").naive


def _apply_tag_changes(existing: Sequence[str] | None, descriptor: UpdateDescriptor) -> list[str] | None:
    if not descriptor.add_tags and not descriptor.remove_tags:
        return None
    normalized = {tag.strip() for tag in existing or [] if tag.strip()}
    additions = {tag.strip() for tag in descriptor.add_tags if tag.strip()}
    removals = {tag.strip() for tag in descriptor.remove_tags if tag.strip()}
    normalized.update(additions)
    normalized.difference_update(removals)
    return sorted(normalized)


def _has_update_candidates(descriptor: UpdateDescriptor) -> bool:
    return bool(
        descriptor.summary
        or descriptor.priority is not None
        or descriptor.status
        or descriptor.x_properties
        or descriptor.project is not None  # Empty string means "unset"
        or descriptor.due is not None  # Empty string means "unset"
        or descriptor.wait is not None  # Empty string means "unset"
        or descriptor.add_tags
        or descriptor.remove_tags
    )


def _build_payload(descriptor: UpdateDescriptor) -> TaskPayload:
    summary = descriptor.summary or descriptor.description
    due = _resolve_due_value(descriptor.due)
    wait = _resolve_due_value(descriptor.wait)
    x_properties = dict(descriptor.x_properties)
    raw_categories = x_properties.pop("CATEGORIES", None)
    metadata_categories = _split_categories_value(raw_categories)
    base_categories = metadata_categories if raw_categories is not None else None
    tags_value = _apply_tag_changes(base_categories, descriptor)
    if tags_value is not None:
        categories = tags_value
    else:
        categories = base_categories
    if descriptor.project:
        x_properties["X-PROJECT"] = descriptor.project
    return TaskPayload(
        summary=summary,
        priority=descriptor.priority,
        due=due,
        wait=wait,
        status=descriptor.status or "IN-PROCESS",
        x_properties=x_properties,
        categories=categories if categories else None,
    )


def _build_patch_from_descriptor(
    descriptor: UpdateDescriptor, existing: Task | None
) -> TaskPatch:
    # Handle empty string as "unset" using sentinel datetime
    if descriptor.due == "":
        due = _UNSET_DATETIME
    else:
        due = _resolve_due_value(descriptor.due)
    if descriptor.wait == "":
        wait = _UNSET_DATETIME
    else:
        wait = _resolve_due_value(descriptor.wait)
    patch = TaskPatch(
        summary=descriptor.summary,
        priority=descriptor.priority,  # 0 signals unset
        due=due,
        wait=wait,
        status=descriptor.status,
    )
    x_properties = dict(descriptor.x_properties)
    raw_categories = x_properties.pop("CATEGORIES", None)
    metadata_categories = _split_categories_value(raw_categories)
    metadata_provided = raw_categories is not None
    existing_categories = existing.categories if existing else None
    base_categories = metadata_categories if metadata_provided else existing_categories
    tags_value = _apply_tag_changes(base_categories, descriptor)
    if tags_value is not None:
        patch.categories = tags_value
    elif metadata_provided:
        patch.categories = metadata_categories
    if descriptor.project is not None:
        if descriptor.project:
            x_properties["X-PROJECT"] = descriptor.project
        else:
            x_properties["X-PROJECT"] = ""  # Empty = remove
    patch.x_properties = x_properties
    return patch


async def _delete_many(client: "CalDAVClient", targets: list[str]) -> list[str]:
    deleted: list[str] = []
    for uid in targets:
        await client.delete_task(uid)
        deleted.append(uid)
    return deleted


async def _sorted_tasks(client: "CalDAVClient") -> list[Task]:
    return sorted(await client.list_tasks(), key=_task_sort_key)


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


from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    style: str
    justify: str
    max_width: int
    ellipsize: bool = False


_BASE_COLUMN_SPECS = [
    ColumnSpec("ID", "cyan", "right", 3),
    ColumnSpec("Age", "bright_blue", "right", 4),
    ColumnSpec("Project", "magenta", "left", 12),
    ColumnSpec("Tag", "yellow", "left", 10),
    ColumnSpec("Due", "bright_green", "left", 10),
    ColumnSpec("Description", "white", "left", SUMMARY_WIDTH, ellipsize=True),
    ColumnSpec("Urg", "bright_red", "right", 4),
]
_UID_COLUMN_SPEC = ColumnSpec("UID", "dim", "left", 36)


def _truncate_value(value: str, max_width: int, ellipsize: bool = False) -> str:
    if len(value) <= max_width:
        return value
    if ellipsize and max_width > 3:
        return value[: max_width - 3] + "..."
    return value[:max_width]


def _format_project(task: Task) -> str:
    project = task.x_properties.get("X-PROJECT") or task.x_properties.get("X-TASKS-ORG-ORDER")
    return project or "-"


def _format_tag(task: Task) -> str:
    if task.categories:
        return ",".join(task.categories)
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
        row_styles=["", "on grey23"],
        padding=(0, 1),
    )
    column_specs = list(_BASE_COLUMN_SPECS)
    if show_uids:
        column_specs.append(_UID_COLUMN_SPEC)
    column_lengths: dict[str, int] = {spec.name: len(spec.name) for spec in column_specs}
    rows: list[list[str]] = []
    now = datetime.now()
    sorted_tasks = sorted(tasks, key=_task_sort_key)
    for task in sorted_tasks:
        due_label = _format_due_label(task.due, now)
        project = _format_project(task)
        tag = _format_tag(task)
        due_date = _format_due_date(task.due)
        summary = task.summary or ""
        priority_label = str(task.priority) if task.priority is not None else "-"
        # Use stable task_index for ID column
        id_label = str(task.task_index) if task.task_index is not None else "?"
        values: dict[str, str] = {
            "ID": id_label,
            "Age": due_label,
            "Project": project,
            "Tag": tag,
            "Due": due_date,
            "Description": summary,
            "Urg": priority_label,
        }
        if show_uids:
            values["UID"] = task.uid
        row: list[str] = []
        for spec in column_specs:
            raw_value = values[spec.name]
            trimmed = _truncate_value(raw_value, spec.max_width, ellipsize=spec.ellipsize)
            row.append(trimmed)
            column_lengths[spec.name] = max(column_lengths[spec.name], len(trimmed))
        rows.append(row)
    for spec in column_specs:
        table.add_column(
            spec.name,
            style=spec.style,
            justify=spec.justify,
            min_width=column_lengths[spec.name],
            max_width=spec.max_width,
            no_wrap=True,
        )
    for row in rows:
        table.add_row(*row)
    console.print(table)


_COMMAND_NAMES = {"add", "config", "del", "do", "list", "modify", "pull", "push", "show", "sync"}


def _looks_like_filter_token(value: str) -> bool:
    if not value:
        return False
    segments = [segment.strip() for segment in value.split(",")]
    normalized = [segment for segment in segments if segment]
    if not normalized:
        return False
    return all(segment.isdigit() for segment in normalized)


def _split_filter_and_command(argv: Sequence[str]) -> tuple[str | None, list[str]]:
    candidates = list(argv)
    if not candidates:
        return None, ["list"]

    # Skip over --env value to find filter/command
    idx = 0
    while idx < len(candidates):
        if candidates[idx] == "--env" and idx + 1 < len(candidates):
            idx += 2  # Skip --env and its value
            continue
        break

    remaining = candidates[idx:]
    prefix = candidates[:idx]

    if not remaining:
        return None, prefix + ["list"]

    first, rest = remaining[0], remaining[1:]
    if first in _COMMAND_NAMES:
        return None, prefix + remaining
    if _looks_like_filter_token(first):
        return first, prefix + (rest or ["list"])
    return None, prefix + remaining


def _parse_filter_indices(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    tokens = [segment.strip() for segment in raw.split(",")]
    normalized = [token for token in tokens if token]
    for token in normalized:
        if not token.isdigit():
            _exit_with_message(f"invalid filter token: {token}")
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
    # Use stable task_index for filtering
    index_map = {str(task.task_index): task for task in tasks if task.task_index is not None}
    selected: list[Task] = []
    for token in indices:
        task = index_map.get(token)
        if task is None:
            _exit_with_message(f"filter {token} did not match any task")
        selected.append(task)
    return selected


def _is_task_completed(task: Task) -> bool:
    status = (task.status or "").strip().upper()
    return status in {"COMPLETED", "DONE"}


def _filter_active_tasks(tasks: list[Task]) -> list[Task]:
    return [task for task in tasks if not _is_task_completed(task)]


def _normalize_tokens(tokens: Sequence[str] | None) -> list[str]:
    return [token for token in tokens or [] if token != "--"]




async def _handle_add(args: argparse.Namespace) -> None:
    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    payload = _build_payload(descriptor)
    client = await _cache_client(args.env)
    try:
        created = await client.create_task(payload)
        print(created.uid)
    finally:
        await client.close()


async def _handle_modify(args: argparse.Namespace) -> None:
    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    if not _has_update_candidates(descriptor):
        _exit_with_message("no changes provided")
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        modified = 0
        for task in tasks:
            patch = _build_patch_from_descriptor(descriptor, task)
            if not patch.has_changes():
                continue
            await client.modify_task(task, patch)
            modified += 1
        if modified == 0:
            _exit_with_message("no changes provided")
        print(f"modified {modified} tasks")
    finally:
        await client.close()


async def _handle_do(args: argparse.Namespace) -> None:
    patch = TaskPatch(status="COMPLETED")
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        completed: list[str] = []
        for task in tasks:
            await client.modify_task(task, patch)
            completed.append(task.uid)
        print(f"marked {len(completed)} tasks as done")
    finally:
        await client.close()


async def _handle_delete(args: argparse.Namespace) -> None:
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        deleted = await _delete_many(client, [task.uid for task in tasks])
        print(f"deleted {len(deleted)} tasks")
    finally:
        await client.close()


async def _handle_list(args: argparse.Namespace) -> None:
    config = _resolve_config(args.env)
    client = await _cache_client(args.env)
    try:
        tasks = await client.list_tasks()
        if not tasks:
            print("no cached tasks found; run 'tdo pull' to synchronize")
            return
        active_tasks = _filter_active_tasks(tasks)
        filtered_tasks = _select_tasks_for_filter(
            active_tasks,
            _effective_filter_indices(args.filter_indices),
        )
        if not filtered_tasks:
            print("no tasks match filter")
            return
        _pretty_print_tasks(filtered_tasks, config.show_uids)
    finally:
        await client.close()


def _format_task_detail(task: Task) -> str:
    lines = []
    lines.append(f"ID:          {task.task_index or '?'}")
    lines.append(f"Summary:     {task.summary}")
    lines.append(f"Status:      {task.status}")
    lines.append(f"Priority:    {task.priority if task.priority is not None else '-'}")
    lines.append(f"Due:         {task.due.isoformat() if task.due else '-'}")
    lines.append(f"Wait:        {task.wait.isoformat() if task.wait else '-'}")

    if task.categories:
        lines.append(f"Tags:        {', '.join(task.categories)}")
    else:
        lines.append("Tags:        -")

    project = task.x_properties.get("X-PROJECT")
    if project:
        lines.append(f"Project:     {project}")

    for key, value in task.x_properties.items():
        if key != "X-PROJECT":
            lines.append(f"{key}: {value}")

    lines.append(f"UID:         {task.uid}")
    if task.href:
        lines.append(f"Href:        {task.href}")

    return "\n".join(lines)


async def _handle_show(args: argparse.Namespace) -> None:
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        for i, task in enumerate(tasks):
            if i > 0:
                print()
            print(_format_task_detail(task))
    finally:
        await client.close()


async def _handle_pull(args: argparse.Namespace) -> None:
    async def _pull(client: "CalDAVClient") -> None:
        result = await client.pull()
        print(f"pulled {result.fetched} tasks")

    await _run_with_client(args.env, _pull)


async def _handle_push(args: argparse.Namespace) -> None:
    async def _push(client: "CalDAVClient") -> None:
        result = await client.push()
        print(
            f"pushed created={result.created} updated={result.updated} deleted={result.deleted}"
        )

    await _run_with_client(args.env, _push)


async def _handle_sync(args: argparse.Namespace) -> None:
    async def _sync(client: "CalDAVClient") -> None:
        result = await client.sync()
        print(
            "sync pulled="
            f"{result.pulled.fetched} created={result.pushed.created}"
            f" updated={result.pushed.updated} deleted={result.pushed.deleted}"
        )

    await _run_with_client(args.env, _sync)


def _handle_config_init(args: argparse.Namespace) -> None:
    target = config_file_path(args.env, args.config_home)
    calendar_url_value = _require_value(args.calendar_url, "CalDAV calendar URL")
    username_value = _require_value(args.username, "CalDAV username")
    password_value = args.password if args.password else None
    token_value = args.token if args.token else None
    env = args.env if args.env else "default"
    config = CaldavConfig(
        calendar_url=calendar_url_value,
        username=username_value,
        password=password_value,
        token=token_value,
        env=env
    )
    try:
        path = write_config_file(target, config, force=args.force)
    except FileExistsError:
        _exit_with_message(f"{target} already exists; use --force to overwrite")
    print(f"created config file at {path}")


def _handle_config_help(args: argparse.Namespace) -> None:
    parser = getattr(args, "parser", None)
    if parser:
        parser.print_help()
    else:
        _exit_with_message("config command requires a subcommand")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tdo")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {_get_version()}"
    )
    parser.add_argument("--env", dest="env", help="env name")
    subparsers = parser.add_subparsers(dest="command")

    add_parser = subparsers.add_parser("add")
    add_parser.add_argument("tokens", nargs=argparse.REMAINDER, default=[], help="taskwarrior tokens")
    add_parser.set_defaults(func=_handle_add)

    modify_parser = subparsers.add_parser("modify")
    modify_parser.add_argument("tokens", nargs=argparse.REMAINDER, default=[], help="taskwarrior tokens")
    modify_parser.set_defaults(func=_handle_modify)

    do_parser = subparsers.add_parser("do")
    do_parser.set_defaults(func=_handle_do)

    delete_parser = subparsers.add_parser("del")
    delete_parser.set_defaults(func=_handle_delete)

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func=_handle_list)

    pull_parser = subparsers.add_parser("pull")
    pull_parser.set_defaults(func=_handle_pull)

    push_parser = subparsers.add_parser("push")
    push_parser.set_defaults(func=_handle_push)

    sync_parser = subparsers.add_parser("sync")
    sync_parser.set_defaults(func=_handle_sync)

    show_parser = subparsers.add_parser("show")
    show_parser.set_defaults(func=_handle_show)

    config_parser = subparsers.add_parser("config")
    config_parser.set_defaults(func=_handle_config_help, parser=config_parser)
    config_subparsers = config_parser.add_subparsers(dest="subcommand")
    init_parser = config_subparsers.add_parser("init")
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


async def _async_main(argv: Sequence[str] | None = None) -> int:
    input_args = list(argv if argv is not None else sys.argv[1:])
    filter_raw, command_tokens = _split_filter_and_command(input_args)
    parser = _build_parser()
    args, remaining = parser.parse_known_args(command_tokens)
    if remaining:
        tokens_value = getattr(args, "tokens", None)
        if tokens_value is not None:
            args.tokens = list(tokens_value) + remaining
        else:
            parser.error(f"unrecognized arguments: {' '.join(remaining)}")
    args.filter_indices = _parse_filter_indices(filter_raw)
    handler = getattr(args, "func", None)
    if handler is None:
        parser.print_help()
        return 0
    if asyncio.iscoroutinefunction(handler):
        await handler(args)
    else:
        handler(args)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_async_main(argv))
