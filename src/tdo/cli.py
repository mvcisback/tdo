from __future__ import annotations

import argparse
import asyncio
import os
import random
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
    resolve_env,
    write_config_file,
)
from .diff import TaskDiff, TaskSetDiff
from .models import Attachment, Task, TaskData, TaskFilter, TaskPatch, TaskPayload
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
        or patch.url is not None
        or patch.attachments
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
        or add.url is not None  # Empty string means "unset"
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
        url=add.url if add.url else None,
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
        url=add.url,  # Empty string signals "unset", None = no change
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
    tasks: list[Task], show_uids: bool, *, title: str | None = None, reverse: bool = False
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
    sorted_tasks = sorted(tasks, key=_task_sort_key, reverse=reverse)
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


_COMMAND_NAMES = {"add", "complete", "config", "del", "do", "list", "modify", "move", "prioritize", "pull", "push", "show", "start", "stop", "sync", "undo"}


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
    from . import tdo_core

    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    payload = _build_payload(descriptor)
    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)

    # Extract fields for tdo_core
    due_str = payload.due.isoformat() if payload.due else None
    wait_str = payload.wait.isoformat() if payload.wait else None
    project = payload.x_properties.get("X-PROJECT") if payload.x_properties else None

    try:
        result = tdo_core.add_task(
            summary=payload.summary or "",
            status=payload.status,
            due=due_str,
            wait=wait_str,
            priority=payload.priority,
            project=project,
            tags=payload.categories,
            url=payload.url,
            env=env_name,
        )
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not result:
        _exit_with_message("failed to add task")

    created = result.to_task()
    diff: TaskSetDiff[int] = TaskSetDiff(
        diffs={created.task_index: TaskDiff(pre=None, post=created.data)}
    )
    print(diff.pretty())

    # Log transaction
    if not diff.is_empty:
        uid_diff = diff.to_uid_keyed(lambda idx: created.uid if idx == created.task_index else str(idx))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="add",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_modify(args: argparse.Namespace) -> None:
    from . import tdo_core

    tokens = _normalize_tokens(args.tokens)
    descriptor = _parse_update_descriptor(tokens)
    if not _has_update_candidates(descriptor):
        _exit_with_message("no changes provided")

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("modify command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before modification for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Build changes from descriptor
    add = descriptor.add_data
    changes_kwargs: dict = {}
    if add.summary:
        changes_kwargs["summary"] = add.summary
    if add.status:
        changes_kwargs["status"] = add.status
    if add.due:
        due_dt = _resolve_due_value(add.due)
        changes_kwargs["due"] = due_dt.isoformat() if due_dt else None
    if add.wait:
        wait_dt = _resolve_due_value(add.wait)
        changes_kwargs["wait"] = wait_dt.isoformat() if wait_dt else None
    if add.priority is not None:
        changes_kwargs["priority"] = add.priority
    if add.x_properties and "X-PROJECT" in add.x_properties:
        changes_kwargs["project"] = add.x_properties["X-PROJECT"]
    if descriptor.add_tags:
        changes_kwargs["add_tags"] = list(descriptor.add_tags)
    if descriptor.remove_tags:
        changes_kwargs["remove_tags"] = list(descriptor.remove_tags)
    if add.url:
        changes_kwargs["url"] = add.url

    if not changes_kwargs:
        _exit_with_message("no changes provided")

    # Perform modification
    try:
        after_tasks = tdo_core.modify_tasks(int_indices, env=env_name, **changes_kwargs)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    before_map = {ct.index: ct.to_task() for ct in before_tasks}
    after_map = {ct.index: ct.to_task() for ct in after_tasks}
    diffs: dict[int, TaskDiff] = {}
    index_to_uid: dict[int, str] = {}
    for idx in int_indices:
        before = before_map.get(idx)
        after = after_map.get(idx)
        if before and after:
            diffs[idx] = TaskDiff(pre=before.data, post=after.data)
            index_to_uid[idx] = before.uid

    if not diffs:
        _exit_with_message("no changes provided")

    result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
    print(result.pretty())

    # Log transaction
    if not result.is_empty:
        uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="modify",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_do(args: argparse.Namespace) -> None:
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("do command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before completion for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Complete tasks
    try:
        tdo_core.complete_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    diffs: dict[int, TaskDiff] = {}
    index_to_uid: dict[int, str] = {}
    for ct in before_tasks:
        task = ct.to_task()
        completed_data = TaskData(
            summary=task.data.summary,
            status="COMPLETED",
            due=task.data.due,
            wait=task.data.wait,
            priority=task.data.priority,
            x_properties=task.data.x_properties,
            categories=task.data.categories,
        )
        diffs[ct.index] = TaskDiff(pre=task.data, post=completed_data)
        index_to_uid[ct.index] = ct.uid

    result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
    print(result.pretty())

    # Log transaction
    if not result.is_empty:
        uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="do",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_start(args: argparse.Namespace) -> None:
    """Start a task: change status from NEEDS-ACTION to IN-PROCESS."""
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("start command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Start tasks
    try:
        after_tasks = tdo_core.start_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    before_map = {ct.index: ct.to_task() for ct in before_tasks}
    after_map = {ct.index: ct.to_task() for ct in after_tasks}
    diffs: dict[int, TaskDiff] = {}
    index_to_uid: dict[int, str] = {}
    for idx in int_indices:
        before = before_map.get(idx)
        after = after_map.get(idx)
        if before and after:
            diffs[idx] = TaskDiff(pre=before.data, post=after.data)
            index_to_uid[idx] = before.uid

    result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
    print(result.pretty())

    # Log transaction
    if not result.is_empty:
        uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="start",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_stop(args: argparse.Namespace) -> None:
    """Stop a task: change status from IN-PROCESS to NEEDS-ACTION."""
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("stop command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Stop tasks
    try:
        after_tasks = tdo_core.stop_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    before_map = {ct.index: ct.to_task() for ct in before_tasks}
    after_map = {ct.index: ct.to_task() for ct in after_tasks}
    diffs: dict[int, TaskDiff] = {}
    index_to_uid: dict[int, str] = {}
    for idx in int_indices:
        before = before_map.get(idx)
        after = after_map.get(idx)
        if before and after:
            diffs[idx] = TaskDiff(pre=before.data, post=after.data)
            index_to_uid[idx] = before.uid

    result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
    print(result.pretty())

    # Log transaction
    if not result.is_empty:
        uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="stop",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_delete(args: argparse.Namespace) -> None:
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("delete command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before deletion for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Delete tasks
    try:
        tdo_core.delete_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    diffs: dict[int, TaskDiff] = {}
    index_to_uid: dict[int, str] = {}
    for ct in before_tasks:
        task = ct.to_task()
        diffs[ct.index] = TaskDiff(pre=task.data, post=None)
        index_to_uid[ct.index] = ct.uid

    result: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
    print(result.pretty())

    # Log transaction
    if not result.is_empty:
        uid_diff = result.to_uid_keyed(lambda idx: index_to_uid.get(idx, str(idx)))
        tdo_core.log_transaction(
            uid_diff.to_json(),
            operation="delete",
            max_entries=config.cache.transaction_log_size,
            env=env_name,
        )


async def _handle_list(args: argparse.Namespace) -> None:
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    task_filter = getattr(args, "task_filter", None)

    # Get all tasks from Rust backend
    try:
        core_tasks = tdo_core.list_tasks(env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not core_tasks:
        if task_filter:
            print("no tasks match filter")
        else:
            print("no cached tasks found; run 'tdo pull' to synchronize")
        return

    # Convert to Task objects
    tasks = [ct.to_task() for ct in core_tasks]

    # Apply task_filter in Python
    if task_filter:
        if task_filter.indices:
            tasks = [t for t in tasks if t.task_index in task_filter.indices]
        if task_filter.project:
            tasks = [t for t in tasks if t.data.x_properties.get("X-PROJECT") == task_filter.project]
        if task_filter.tags:
            tasks = [t for t in tasks if t.data.categories and any(tag in t.data.categories for tag in task_filter.tags)]

    # Filter out waiting tasks (wait date in the future)
    now = datetime.now()
    tasks = [t for t in tasks if not t.data.wait or t.data.wait <= now]

    # Filter out completed tasks (they're in a separate table, but just in case)
    active_tasks = [t for t in tasks if t.data.status != "COMPLETED"]
    if not active_tasks:
        print("no tasks match filter")
        return

    # Split tasks by status: IN-PROCESS (started) and NEEDS-ACTION (backlog)
    started = [t for t in active_tasks if t.data.status == "IN-PROCESS"]
    backlog = [t for t in active_tasks if t.data.status == "NEEDS-ACTION"]
    other = [t for t in active_tasks if t.data.status not in ("IN-PROCESS", "NEEDS-ACTION", "COMPLETED")]

    reverse = not getattr(args, "no_reverse", False)

    # Display order: Backlog first, then Started (so Started appears at bottom)
    if backlog:
        _pretty_print_tasks(backlog, config.show_uids, title="Backlog", reverse=reverse)
    if started:
        if backlog:
            print()  # Blank line between tables
        _pretty_print_tasks(started, config.show_uids, title="Started", reverse=reverse)
    # Handle tasks with other statuses (if any)
    if other:
        if started or backlog:
            print()
        _pretty_print_tasks(other, config.show_uids, title="Other", reverse=reverse)


async def _handle_wait(args: argparse.Namespace) -> None:
    """Show tasks with future wait dates."""
    from . import tdo_core

    config = _resolve_config(args.env)
    env_name = resolve_env(args.env)
    task_filter = getattr(args, "task_filter", None)

    # Get all tasks from Rust backend
    try:
        core_tasks = tdo_core.list_tasks(env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Convert to Task objects
    tasks = [ct.to_task() for ct in core_tasks]

    # Filter to only waiting tasks (wait date in the future)
    now = datetime.now()
    waiting_tasks = [t for t in tasks if t.data.wait and t.data.wait > now]

    # Apply task_filter
    if task_filter:
        if task_filter.indices:
            waiting_tasks = [t for t in waiting_tasks if t.task_index in task_filter.indices]
        if task_filter.project:
            waiting_tasks = [t for t in waiting_tasks if t.data.x_properties.get("X-PROJECT") == task_filter.project]
        if task_filter.tags:
            waiting_tasks = [t for t in waiting_tasks if t.data.categories and any(tag in t.data.categories for tag in task_filter.tags)]

    if not waiting_tasks:
        print("no waiting tasks")
        return
    _pretty_print_tasks(waiting_tasks, config.show_uids, title="Waiting")


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

    if task.data.url:
        lines.append(f"URL:         {task.data.url}")

    if task.data.attachments:
        lines.append(f"Attachments: {len(task.data.attachments)}")
        for i, attach in enumerate(task.data.attachments, 1):
            fmttype_display = f" ({attach.fmttype})" if attach.fmttype else ""
            lines.append(f"  [{i}] {attach.uri}{fmttype_display}")

    for key, value in task.data.x_properties.items():
        if key != "X-PROJECT":
            lines.append(f"{key}: {value}")

    lines.append(f"UID:         {task.uid}")
    if task.href:
        lines.append(f"Href:        {task.href}")

    return "\n".join(lines)


async def _handle_show(args: argparse.Namespace) -> None:
    from . import tdo_core

    env_name = resolve_env(args.env)
    indices = _effective_filter_indices(args.filter_indices)

    if not indices:
        _exit_with_message("show command requires task indices")

    # Convert string indices to int
    int_indices = [int(i) for i in indices]

    try:
        core_tasks = tdo_core.show_tasks(int_indices, env=env_name)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not core_tasks:
        _exit_with_message("no tasks match filter")

    tasks = [ct.to_task() for ct in core_tasks]

    for i, task in enumerate(tasks):
        if i > 0:
            print()
        print(_format_task_detail(task))


async def _handle_attach(args: argparse.Namespace) -> None:
    """Add, remove, or list attachments on a task."""
    client = await _cache_client(args.env)
    try:
        tasks = _select_tasks_for_filter(
            await _sorted_tasks(client),
            _effective_filter_indices(args.filter_indices),
        )
        if not tasks:
            _exit_with_message("no tasks match filter")
        if len(tasks) > 1:
            _exit_with_message("attach command requires exactly one task")

        task = tasks[0]

        # List mode
        if args.list_only:
            if not task.data.attachments:
                print("No attachments")
            else:
                print(f"Attachments ({len(task.data.attachments)}):")
                for i, attach in enumerate(task.data.attachments, 1):
                    fmttype_display = f" ({attach.fmttype})" if attach.fmttype else ""
                    print(f"  [{i}] {attach.uri}{fmttype_display}")
            return

        # Require URL for add/remove
        if not args.url:
            _exit_with_message("attach command requires a URL argument")

        # Remove mode
        if args.remove:
            existing_attachments = list(task.data.attachments)
            new_attachments = [a for a in existing_attachments if a.uri != args.url]
            if len(new_attachments) == len(existing_attachments):
                _exit_with_message(f"attachment not found: {args.url}")
            # Create a new task with filtered attachments
            updated_data = TaskData(
                summary=task.data.summary,
                status=task.data.status,
                due=task.data.due,
                wait=task.data.wait,
                priority=task.data.priority,
                x_properties=task.data.x_properties,
                categories=task.data.categories,
                url=task.data.url,
                attachments=new_attachments,
            )
            updated = Task(
                uid=task.uid,
                data=updated_data,
                href=task.href,
                task_index=task.task_index,
            )
            pending_action = await client.cache.get_pending_action(task.uid) if client.cache else None
            action = "create" if pending_action == "create" else "update"
            await client.cache.upsert_task(updated, pending_action=action)
            diff = TaskDiff(pre=task.data, post=updated.data)
            print(TaskSetDiff(diffs={task.task_index: diff}).pretty())
        else:
            # Add mode
            new_attachment = Attachment(uri=args.url, fmttype=args.fmttype)
            patch = TaskPatch(attachments=[new_attachment])
            updated = await client.modify_task(task, patch)
            diff = TaskDiff(pre=task.data, post=updated.data)
            print(TaskSetDiff(diffs={task.task_index: diff}).pretty())

        # Log transaction
        if client.cache:
            uid_diff = TaskSetDiff(diffs={task.uid: diff})
            await client.cache.log_transaction(
                uid_diff,
                operation="attach",
                max_entries=client.config.cache.transaction_log_size,
            )
    finally:
        await client.close()


async def _handle_prioritize(args: argparse.Namespace) -> None:
    """Interactive prioritization of tasks."""
    client = await _cache_client(args.env)
    task_filter = getattr(args, "task_filter", None)
    sampling_all = False

    try:
        while True:
            # Get unprioritized tasks (with filter)
            unprioritized = await client.cache.list_unprioritized_tasks(
                task_filter=task_filter
            )

            if unprioritized:
                task = random.choice(unprioritized)
                show_priority = False
            else:
                # All prioritized - sample from all active tasks
                all_tasks = await client.list_active_tasks(
                    exclude_waiting=True,
                    task_filter=task_filter,
                )
                if not all_tasks:
                    print("No tasks to prioritize")
                    return
                if not sampling_all:
                    print("\nAll tasks prioritized! Sampling from all tasks...\n")
                    sampling_all = True
                task = random.choice(all_tasks)
                show_priority = True

            # Display task
            pri_display = f" (pri: {task.data.priority})" if show_priority and task.data.priority else ""
            print(f"[{task.task_index}] {task.data.summary}{pri_display}")

            # Prompt for priority
            try:
                response = input("Priority (H/M/L/1-9, q=quit): ").strip().lower()
            except EOFError:
                return

            if response == "q":
                return

            priority = _parse_priority(response)
            if priority is None:
                print("Invalid priority, try again\n")
                continue

            # Update task
            patch = TaskPatch(priority=priority)
            await client.modify_task(task, patch)
            print(f"-> Set priority to {priority}\n")
    finally:
        await client.close()


async def _handle_move(args: argparse.Namespace) -> None:
    """Move tasks from current environment to destination environment."""
    from . import tdo_core

    dest_env = args.dest_env
    source_env = args.env

    # Validate source != destination
    source_resolved = resolve_env(source_env)
    if source_resolved == dest_env:
        _exit_with_message(f"cannot move tasks to the same environment: {dest_env}")

    # Validate destination environment config exists
    try:
        dest_config = load_config(dest_env)
        source_config = _resolve_config(source_env)
    except (RuntimeError, FileNotFoundError) as e:
        _exit_with_message(f"environment not configured: {e}")

    indices = _effective_filter_indices(args.filter_indices)
    if not indices:
        _exit_with_message("move command requires task indices")

    int_indices = [int(i) for i in indices]

    # Get tasks before move for diff
    try:
        before_tasks = tdo_core.show_tasks(int_indices, env=source_resolved)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    if not before_tasks:
        _exit_with_message("no tasks match filter")

    # Build index to uid mapping for source
    index_to_uid: dict[int, str] = {ct.index: ct.uid for ct in before_tasks}

    # Move tasks
    try:
        moved_tasks = tdo_core.move_tasks(int_indices, dest_env, env=source_resolved)
    except tdo_core.TdoCoreError as e:
        _exit_with_message(f"error: {e}")

    # Build diffs
    source_diffs: dict[int, TaskDiff] = {}
    dest_diffs: dict[int, TaskDiff] = {}
    dest_index_to_uid: dict[int, str] = {}

    for before_ct in before_tasks:
        src_task = before_ct.to_task()
        source_diffs[before_ct.index] = TaskDiff(pre=src_task.data, post=None)

    for dest_ct in moved_tasks:
        dest_task = dest_ct.to_task()
        dest_diffs[dest_ct.index] = TaskDiff(pre=None, post=dest_task.data)
        dest_index_to_uid[dest_ct.index] = dest_ct.uid

    # Display results
    print(f"Moved {len(moved_tasks)} task(s) from '{source_resolved}' to '{dest_env}':")
    for before_ct, dest_ct in zip(before_tasks, moved_tasks):
        print(f"  [{before_ct.index}] {before_ct.summary} -> [{dest_ct.index}] in {dest_env}")

    # Log transaction for source environment
    if source_diffs:
        source_uid_diff = TaskSetDiff(diffs=source_diffs).to_uid_keyed(
            lambda idx: index_to_uid.get(idx, str(idx))
        )
        tdo_core.log_transaction(
            source_uid_diff.to_json(),
            operation="move-out",
            max_entries=source_config.cache.transaction_log_size,
            env=source_resolved,
        )

    # Log transaction for destination environment
    if dest_diffs:
        dest_uid_diff = TaskSetDiff(diffs=dest_diffs).to_uid_keyed(
            lambda idx: dest_index_to_uid.get(idx, str(idx))
        )
        tdo_core.log_transaction(
            dest_uid_diff.to_json(),
            operation="move-in",
            max_entries=dest_config.cache.transaction_log_size,
            env=dest_env,
        )


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


async def _handle_complete(args: argparse.Namespace) -> None:
    """Output completion data for shell autocompletion."""
    complete_type = args.complete_type

    if complete_type == "envs":
        # List available environment names from config files
        config_home = Path.home() / ".config" / "tdo"
        if config_home.exists():
            for f in config_home.glob("config.*.toml"):
                # Extract env name from config.<env>.toml
                name = f.stem  # config.<env>
                if name.startswith("config."):
                    env_name = name[7:]  # Remove "config." prefix
                    print(env_name)

    elif complete_type == "tasks":
        # List task indices with summaries
        try:
            client = await _cache_client(args.env)
            try:
                tasks = await client.list_active_tasks(exclude_waiting=False)
                for task in tasks:
                    if task.task_index is not None:
                        summary = (task.data.summary or "")[:50]
                        print(f"{task.task_index}\t{summary}")
            finally:
                await client.close()
        except Exception:
            pass  # Silently fail for completions

    elif complete_type == "projects":
        # List unique project names
        try:
            client = await _cache_client(args.env)
            try:
                tasks = await client.list_active_tasks(exclude_waiting=False)
                projects: set[str] = set()
                for task in tasks:
                    proj = task.data.x_properties.get("X-PROJECT")
                    if proj:
                        projects.add(proj)
                for proj in sorted(projects):
                    print(proj)
            finally:
                await client.close()
        except Exception:
            pass

    elif complete_type == "tags":
        # List unique tag names
        try:
            client = await _cache_client(args.env)
            try:
                tasks = await client.list_active_tasks(exclude_waiting=False)
                tags: set[str] = set()
                for task in tasks:
                    if task.data.categories:
                        tags.update(task.data.categories)
                for tag in sorted(tags):
                    print(tag)
            finally:
                await client.close()
        except Exception:
            pass


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
    list_parser.add_argument(
        "--no-reverse",
        action="store_true",
        dest="no_reverse",
        help="disable reversed sort order (highest priority at top)",
    )
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

    attach_parser = subparsers.add_parser("attach")
    attach_parser.add_argument("url", nargs="?", help="attachment URL")
    attach_parser.add_argument("--fmttype", dest="fmttype", help="MIME type for attachment")
    attach_parser.add_argument("--remove", dest="remove", action="store_true", help="remove attachment")
    attach_parser.add_argument("--list", dest="list_only", action="store_true", help="list attachments")
    attach_parser.set_defaults(func=_handle_attach)

    prioritize_parser = subparsers.add_parser("prioritize")
    prioritize_parser.set_defaults(func=_handle_prioritize)

    move_parser = subparsers.add_parser("move")
    move_parser.add_argument("dest_env", help="destination environment name")
    move_parser.set_defaults(func=_handle_move)

    complete_parser = subparsers.add_parser("complete", help="output completion data for shell autocompletion")
    complete_parser.add_argument("complete_type", choices=["envs", "tasks", "projects", "tags"], help="type of completion data")
    complete_parser.set_defaults(func=_handle_complete)

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
