from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Sequence, TypeVar

import typer

from .caldav_client import CalDAVClient
from .config import load_config
from .models import TaskPatch, TaskPayload


T = TypeVar("T")
app = typer.Typer()
_CLIENT_FACTORY: type[CalDAVClient] = CalDAVClient


def _run_with_client(env: str | None, callback: Callable[[CalDAVClient], Awaitable[T]]) -> T:
    config = load_config(env)

    async def runner() -> T:
        async with _CLIENT_FACTORY(config) as client:
            return await callback(client)

    return asyncio.run(runner())


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


def _delete_many(client: CalDAVClient, targets: list[str]) -> Awaitable[list[str]]:
    async def runner() -> list[str]:
        deleted: list[str] = []
        for uid in targets:
            await client.delete_task(uid)
            deleted.append(uid)
        return deleted

    return runner()


@app.command()
def add(
    description: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    tokens: list[str] | None = typer.Argument(None),
):
    payload = _parse_payload(description, tokens or ())
    created = _run_with_client(env, lambda client: client.create_task(payload))
    typer.echo(created.uid)


@app.command()
def modify(
    uid: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
    tokens: list[str] | None = typer.Argument(None),
):
    patch = _parse_patch(tokens or ())
    if not patch.has_changes():
        typer.echo("no changes provided")
        raise typer.Exit(code=1)
    updated = _run_with_client(env, lambda client: client.modify_task(uid, patch))
    typer.echo(updated.uid)


@app.command(name="del")
def delete(
    uids: str,
    env: str | None = typer.Option(None, "--env", help="env name"),
):
    targets = [token.strip() for token in uids.split(",") if token.strip()]
    if not targets:
        typer.echo("no targets provided")
        raise typer.Exit(code=1)
    deleted = _run_with_client(env, lambda client: _delete_many(client, targets))
    typer.echo(f"deleted {len(deleted)} tasks")


@app.command(name="list")
def list_tasks(env: str | None = typer.Option(None, "--env", help="env name")) -> None:
    tasks = _run_with_client(env, lambda client: client.list_tasks())
    if not tasks:
        typer.echo("no tasks found")
        return
    for task in tasks:
        due = task.due.isoformat() if task.due else "-"
        typer.echo(f"{task.uid}\t{due}\t{task.summary}")
