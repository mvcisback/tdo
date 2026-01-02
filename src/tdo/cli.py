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
from .diff import TaskDiff, TaskSetDiff
from .models import Task, TaskData, TaskFilter, TaskPatch, TaskPayload
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


def _has_changes(patch: TaskData) -> bool:
    return bool(
        patch.summary
        or patch.status
        or patch.priority is not None
        or patch.due is not None
        or patch.wait is not None
        or patch.x_properties
        or patch.categories is not None
    )


def _truncate_summary(summary: str, max_len: int = 30) -> str:
    if len(summary) <= max_len:
        return summary
    return summary[: max_len - 3] + "..."


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
    add_tags = descriptor.add_data.categories or []
    remove_tags = descriptor.remove_data.categories or []
    if not add_tags and not remove_tags:
        return None
    normalized = {tag.strip() for tag in existing or [] if tag.strip()}
    additions = {tag.strip() for tag in add_tags if tag.strip()}
    removals = {tag.strip() for tag in remove_tags if tag.strip()}
    normalized.update(additions)
    normalized.difference_update(removals)
    return sorted(normalized)


def _has_update_candidates(descriptor: UpdateDescriptor) -> bool:
    add = descriptor.add_data
    remove = descriptor.remove_data
    return bool(
        add.summary
        or add.priority is not None
        or add.status
        or add.x_properties
        or add.due is not None  # Empty string means "unset"
        or add.wait is not None  # Empty string means "unset"
        or add.categories
        or remove.categories
    )


def _build_payload(descriptor: UpdateDescriptor) -> TaskPayload:
    add = descriptor.add_data
    summary = add.summary
    due = _resolve_due_value(add.due)
    wait = _resolve_due_value(add.wait)
    x_properties = dict(add.x_properties)
    raw_categories = x_properties.pop("CATEGORIES", None)
    metadata_categories = _split_categories_value(raw_categories)
    base_categories = metadata_categories if raw_categories is not None else None
    tags_value = _apply_tag_changes(base_categories, descriptor)
    if tags_value is not None:
        categories = tags_value
    else:
        categories = base_categories
    return TaskPayload(
        summary=summary,
        priority=add.priority,
        due=due,
        wait=wait,
        status=add.status or "NEEDS-ACTION",
        x_properties=x_properties,
        categories=categories if categories else None,
    )


def _build_patch_from_descriptor(
    descriptor: UpdateDescriptor, existing: Task | None
) -> TaskPatch:
    add = descriptor.add_data
    # Handle empty string as "unset" using sentinel datetime
    if add.due == "":
        due = _UNSET_DATETIME
    else:
        due = _resolve_due_value(add.due)
    if add.wait == "":
        wait = _UNSET_DATETIME
    else:
        wait = _resolve_due_value(add.wait)
    patch = TaskPatch(
        summary=add.summary,
        priority=add.priority,  # 0 signals unset
        due=due,
        wait=wait,
        status=add.status,
    )
    x_properties = dict(add.x_properties)
    raw_categories = x_properties.pop("CATEGORIES", None)
    metadata_categories = _split_categories_value(raw_categories)
    metadata_provided = raw_categories is not None
    existing_categories = existing.data.categories if existing else None
    base_categories = metadata_categories if metadata_provided else existing_categories
    tags_value = _apply_tag_changes(base_categories, descriptor)
    if tags_value is not None:
        patch.categories = tags_value
    elif metadata_provided:
        patch.categories = metadata_categories
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
    due_key = task.data.due or datetime.max
    priority_key = task.data.priority if task.data.priority is not None else 10
    summary_key = task.data.summary.strip().lower() if task.data.summary else ""
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
    project = task.data.x_properties.get("X-PROJECT") or task.data.x_properties.get("X-TASKS-ORG-ORDER")
    return project or "-"


def _format_tag(task: Task) -> str:
    if task.data.categories:
        return ",".join(task.data.categories)
    tag = task.data.x_properties.get("X-TAG") or task.data.x_properties.get("X-COLOR")
    return tag or "-"


def _format_due_date(due: datetime | None) -> str:
    if not due:
        return "-"
    return due.strftime("%Y-%m-%d")


def _pretty_print_tasks(
    tasks: list[Task], show_uids: bool, *, title: str | None = None
) -> None:
    console = Console(file=sys.stdout, color_system="auto")
    table = Table(
        title=title,
        title_style="bold",
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
        due_label = _format_due_label(task.data.due, now)
        project = _format_project(task)
        tag = _format_tag(task)
        due_date = _format_due_date(task.data.due)
        summary = task.data.summary or ""
        priority_label = str(task.data.priority) if task.data.priority is not None else "-"
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


_COMMAND_NAMES = {"add", "config", "del", "do", "list", "modify", "pull", "push", "show", "start", "stop", "sync", "undo"}


def _looks_like_index_filter(value: str) -> bool:
    """Check if value looks like numeric indices (e.g., '1,2,3')."""
    if not value:
        return False
    segments = [segment.strip() for segment in value.split(",")]
    normalized = [segment for segment in segments if segment]
    if not normalized:
        return False
    return all(segment.isdigit() for segment in normalized)


def _looks_like_metadata_filter(value: str) -> bool:
    """Check if value looks like a metadata filter (project:X, +tag)."""
    if not value:
        return False
    # +tag or -tag (for filtering)
    if value.startswith("+") and len(value) > 1:
        return True
    # project:value
    if value.startswith("project:"):
        return True
    return False


def _looks_like_filter_token(value: str) -> bool:
    """Check if value is any kind of filter token."""
    return _looks_like_index_filter(value) or _looks_like_metadata_filter(value)


def _split_filter_and_command(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split argv into filter tokens and command tokens.

    Returns (filter_tokens, command_tokens) where filter_tokens can include:
    - Numeric indices: "1,2,3"
    - Project filter: "project:tdo"
    - Tag filter: "+easy"
    """
    candidates = list(argv)
    if not candidates:
        return [], ["list"]

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
        return [], prefix + ["list"]

    # Collect all filter tokens before the command
    filter_tokens: list[str] = []
    while remaining:
        token = remaining[0]
        if token in _COMMAND_NAMES:
            break
        if _looks_like_filter_token(token):
            filter_tokens.append(token)
            remaining = remaining[1:]
        else:
            break

    if not remaining:
        remaining = ["list"]

    return filter_tokens, prefix + remaining


def _parse_task_filter(tokens: list[str]) -> TaskFilter | None:
    """Parse filter tokens into a TaskFilter object."""
    if not tokens:
        return None

    project: str | None = None
    tags: list[str] = []
    indices: list[int] = []

    for token in tokens:
        if token.startswith("project:"):
            project = token[8:]  # len("project:") = 8
        elif token.startswith("+") and len(token) > 1:
            tags.append(token[1:])
        elif _looks_like_index_filter(token):
            for segment in token.split(","):
                segment = segment.strip()
                if segment.isdigit():
                    indices.append(int(segment))

    if not project and not tags and not indices:
        return None

    return TaskFilter(project=project, tags=tags, indices=indices)


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
    status = (task.data.status or "").strip().upper()
    return status in {"COMPLETED", "DONE"}


def _filter_active_tasks(tasks: list[Task]) -> list[Task]:
    return [task for task in tasks if not _is_task_completed(task)]


def _is_task_waiting(task: Task) -> bool:
    """Return True if task has a wait date in the future."""
    if task.data.wait is None:
        return False
    return task.data.wait > datetime.now(task.data.wait.tzinfo)


def _normalize_tokens(tokens: Sequence[str] | None) -> list[str]:
    return [token for token in tokens or [] if token != "--"]




async def _handle_add(args: argparse.Namespace) -> None:
    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    payload = _build_payload(descriptor)
    client = await _cache_client(args.env)
    try:
        created = await client.create_task(payload)
        diff: TaskSetDiff[int] = TaskSetDiff(
            diffs={created.task_index: TaskDiff(pre=None, post=created.data)}
        )
        print(diff.pretty())

        # Log transaction
        if not diff.is_empty and client.cache:
            uid_diff = diff.to_uid_keyed(lambda idx: created.uid if idx == created.task_index else str(idx))
            await client.cache.log_transaction(
                uid_diff,
                operation="add",
                max_entries=client.config.cache.transaction_log_size,
            )
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
        diffs: dict[int, TaskDiff] = {}
        index_to_uid: dict[int, str] = {}
        for task in tasks:
            patch = _build_patch_from_descriptor(descriptor, task)
            if not _has_changes(patch):
                continue
            updated = await client.modify_task(task, patch)
            diffs[task.task_index] = TaskDiff(pre=task.data, post=updated.data)
            index_to_uid[task.task_index] = task.uid
        if not diffs:
            _exit_with_message("no changes provided")
        result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
        print(result.pretty())

        # Log transaction
        if not result.is_empty and client.cache:
            uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
            await client.cache.log_transaction(
                uid_diff,
                operation="modify",
                max_entries=client.config.cache.transaction_log_size,
            )
    finally:
        await client.close()


async def _handle_do(args: argparse.Namespace) -> None:
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        diffs: dict[int, TaskDiff] = {}
        index_to_uid: dict[int, str] = {}
        for task in tasks:
            # Use complete_task to move task to completed_tasks table
            await client.complete_task(task.uid)
            # Build diff with original data -> completed status
            completed_data = TaskData(
                summary=task.data.summary,
                status="COMPLETED",
                due=task.data.due,
                wait=task.data.wait,
                priority=task.data.priority,
                x_properties=task.data.x_properties,
                categories=task.data.categories,
            )
            diffs[task.task_index] = TaskDiff(pre=task.data, post=completed_data)
            index_to_uid[task.task_index] = task.uid
        result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
        print(result.pretty())

        # Log transaction
        if not result.is_empty and client.cache:
            uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
            await client.cache.log_transaction(
                uid_diff,
                operation="do",
                max_entries=client.config.cache.transaction_log_size,
            )
    finally:
        await client.close()


async def _change_status(args: argparse.Namespace, status: str, operation: str) -> None:
    """Change task status and log the transaction."""
    patch = TaskPatch(status=status)
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        diffs: dict[int, TaskDiff] = {}
        index_to_uid: dict[int, str] = {}
        for task in tasks:
            updated = await client.modify_task(task, patch)
            diffs[task.task_index] = TaskDiff(pre=task.data, post=updated.data)
            index_to_uid[task.task_index] = task.uid
        result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
        print(result.pretty())

        # Log transaction
        if not result.is_empty and client.cache:
            uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
            await client.cache.log_transaction(
                uid_diff,
                operation=operation,
                max_entries=client.config.cache.transaction_log_size,
            )
    finally:
        await client.close()


async def _handle_start(args: argparse.Namespace) -> None:
    """Start a task: change status from NEEDS-ACTION to IN-PROCESS."""
    await _change_status(args, "IN-PROCESS", "start")


async def _handle_stop(args: argparse.Namespace) -> None:
    """Stop a task: change status from IN-PROCESS to NEEDS-ACTION."""
    await _change_status(args, "NEEDS-ACTION", "stop")


async def _handle_delete(args: argparse.Namespace) -> None:
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        diffs: dict[int, TaskDiff] = {}
        index_to_uid: dict[int, str] = {}
        for task in tasks:
            await client.delete_task(task.uid)
            diffs[task.task_index] = TaskDiff(pre=task.data, post=None)
            index_to_uid[task.task_index] = task.uid
        result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
        print(result.pretty())

        # Log transaction
        if not result.is_empty and client.cache:
            uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
            await client.cache.log_transaction(
                uid_diff,
                operation="delete",
                max_entries=client.config.cache.transaction_log_size,
            )
    finally:
        await client.close()


async def _handle_list(args: argparse.Namespace) -> None:
    config = _resolve_config(args.env)
    client = await _cache_client(args.env)
    try:
        # Use filtered query if metadata filters are provided
        task_filter = getattr(args, "task_filter", None)
        if task_filter:
            tasks = await client.list_tasks_filtered(task_filter)
        else:
            tasks = await client.list_tasks()
        if not tasks:
            if task_filter:
                print("no tasks match filter")
            else:
                print("no cached tasks found; run 'tdo pull' to synchronize")
            return
        active_tasks = _filter_active_tasks(tasks)
        # Filter out tasks with future wait dates
        active_tasks = [t for t in active_tasks if not _is_task_waiting(t)]
        if not active_tasks:
            print("no tasks match filter")
            return

        # Split tasks by status: IN-PROCESS (started) and NEEDS-ACTION (backlog)
        started = [t for t in active_tasks if t.data.status == "IN-PROCESS"]
        backlog = [t for t in active_tasks if t.data.status == "NEEDS-ACTION"]

        if started:
            _pretty_print_tasks(started, config.show_uids, title="Started")
        if backlog:
            if started:
                print()  # Blank line between tables
            _pretty_print_tasks(backlog, config.show_uids, title="Backlog")
        # Handle tasks with other statuses (if any)
        other = [t for t in active_tasks if t.data.status not in ("IN-PROCESS", "NEEDS-ACTION", "COMPLETED")]
        if other:
            if started or backlog:
                print()
            _pretty_print_tasks(other, config.show_uids, title="Other")
    finally:
        await client.close()


async def _handle_wait(args: argparse.Namespace) -> None:
    """Show tasks with future wait dates."""
    config = _resolve_config(args.env)
    client = await _cache_client(args.env)
    try:
        task_filter = getattr(args, "task_filter", None)
        if task_filter:
            tasks = await client.list_tasks_filtered(task_filter)
        else:
            tasks = await client.list_tasks()
        if not tasks:
            if task_filter:
                print("no tasks match filter")
            else:
                print("no cached tasks found; run 'tdo pull' to synchronize")
            return
        active_tasks = _filter_active_tasks(tasks)
        # Only show tasks with future wait dates
        waiting_tasks = [t for t in active_tasks if _is_task_waiting(t)]
        if not waiting_tasks:
            print("no waiting tasks")
            return
        _pretty_print_tasks(waiting_tasks, config.show_uids, title="Waiting")
    finally:
        await client.close()


def _format_task_detail(task: Task) -> str:
    lines = []
    lines.append(f"ID:          {task.task_index or '?'}")
    lines.append(f"Summary:     {task.data.summary}")
    lines.append(f"Status:      {task.data.status}")
    lines.append(f"Priority:    {task.data.priority if task.data.priority is not None else '-'}")
    lines.append(f"Due:         {task.data.due.isoformat() if task.data.due else '-'}")
    lines.append(f"Wait:        {task.data.wait.isoformat() if task.data.wait else '-'}")

    if task.data.categories:
        lines.append(f"Tags:        {', '.join(task.data.categories)}")
    else:
        lines.append("Tags:        -")

    project = task.data.x_properties.get("X-PROJECT")
    if project:
        lines.append(f"Project:     {project}")

    for key, value in task.data.x_properties.items():
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
        if result.diff.is_empty:
            print(f"pulled {result.fetched} tasks (no changes)")
        else:
            print(result.diff.pretty())

    await _run_with_client(args.env, _pull)


async def _handle_push(args: argparse.Namespace) -> None:
    async def _push(client: "CalDAVClient") -> None:
        result = await client.push()
        if result.diff.is_empty:
            print("nothing to push")
        else:
            print(result.diff.pretty())

    await _run_with_client(args.env, _push)


async def _handle_sync(args: argparse.Namespace) -> None:
    async def _sync(client: "CalDAVClient") -> None:
        result = await client.sync()
        pull_empty = result.pulled.diff.is_empty
        push_empty = result.pushed.diff.is_empty

        if pull_empty and push_empty:
            print("already in sync")
        else:
            if not pull_empty:
                print("Pulled:")
                print(result.pulled.diff.pretty())
            if not push_empty:
                if not pull_empty:
                    print()
                print("Pushed:")
                print(result.pushed.diff.pretty())

    await _run_with_client(args.env, _sync)


async def _handle_undo(args: argparse.Namespace) -> None:
    client = await _cache_client(args.env)
    try:
        cache = client._ensure_cache()

        # Pop newest entry
        entry = await cache.pop_transaction()
        if entry is None:
            _exit_with_message("no transactions to undo")

        # Deserialize the diff
        original_diff = TaskSetDiff.from_json(entry.diff_json)
        inverse_diff = original_diff.inv()
        operation = entry.operation

        # Apply undo based on operation type
        for uid, diff in inverse_diff.diffs.items():
            if diff.is_noop:
                continue

            if operation == "do":
                # Undo complete: move from completed_tasks back to tasks
                # The inverse diff has is_update with post.status != COMPLETED
                if diff.is_update and diff.post and diff.post.status != "COMPLETED":
                    await cache.restore_from_completed(uid, status=diff.post.status or "NEEDS-ACTION")

            elif operation == "delete":
                # Undo delete: restore from deleted_tasks to tasks
                if diff.is_create:
                    deleted_task = await cache.get_deleted_task(uid)
                    if deleted_task:
                        await cache.restore_from_deleted(uid)
                    else:
                        # Task was already pushed and flushed, use as_sql fallback
                        sql_statements = TaskSetDiff(diffs={uid: diff}).as_sql()
                        for sql, params in sql_statements:
                            await cache._conn.execute(sql, params)
                        await cache._conn.commit()

            elif operation == "add":
                # Undo add: delete from tasks
                if diff.is_delete:
                    await cache.delete_task(uid)

            else:
                # Fallback for modify and other operations: use as_sql
                sql_statements = TaskSetDiff(diffs={uid: diff}).as_sql()
                for sql, params in sql_statements:
                    await cache._conn.execute(sql, params)
                await cache._conn.commit()

        # Display what was undone
        print(f"Undid {operation or 'operation'}:")
        print(inverse_diff.pretty())
    finally:
        await client.close()


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

    start_parser = subparsers.add_parser("start")
    start_parser.set_defaults(func=_handle_start)

    stop_parser = subparsers.add_parser("stop")
    stop_parser.set_defaults(func=_handle_stop)

    delete_parser = subparsers.add_parser("del")
    delete_parser.set_defaults(func=_handle_delete)

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func=_handle_list)

    waiting_parser = subparsers.add_parser("waiting")
    waiting_parser.set_defaults(func=_handle_wait)

    pull_parser = subparsers.add_parser("pull")
    pull_parser.set_defaults(func=_handle_pull)

    push_parser = subparsers.add_parser("push")
    push_parser.set_defaults(func=_handle_push)

    sync_parser = subparsers.add_parser("sync")
    sync_parser.set_defaults(func=_handle_sync)

    show_parser = subparsers.add_parser("show")
    show_parser.set_defaults(func=_handle_show)

    undo_parser = subparsers.add_parser("undo")
    undo_parser.set_defaults(func=_handle_undo)

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
    filter_tokens, command_tokens = _split_filter_and_command(input_args)
    parser = _build_parser()
    args, remaining = parser.parse_known_args(command_tokens)
    if remaining:
        tokens_value = getattr(args, "tokens", None)
        if tokens_value is not None:
            args.tokens = list(tokens_value) + remaining
        else:
            parser.error(f"unrecognized arguments: {' '.join(remaining)}")
    # Parse filter tokens into TaskFilter
    args.task_filter = _parse_task_filter(filter_tokens)
    # Backward compatibility: extract indices for commands that use filter_indices
    if args.task_filter and args.task_filter.indices:
        args.filter_indices = [str(i) for i in args.task_filter.indices]
    else:
        args.filter_indices = None
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
