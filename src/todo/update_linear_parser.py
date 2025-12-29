from __future__ import annotations

from .update_descriptor import UpdateDescriptor

__all__ = ["parse_update"]


def parse_update(raw: str) -> UpdateDescriptor:
    tokens = [segment for segment in raw.strip().split() if segment]
    index: int | None = None
    additions: list[str] = []
    removals: list[str] = []
    project: str | None = None
    due: str | None = None
    wait: str | None = None

    for position, token in enumerate(tokens):
        if position == 0 and token.isdigit():
            index = int(token)
            continue
        if token.startswith("+") and len(token) > 1:
            additions.append(token[1:])
            continue
        if token.startswith("-") and len(token) > 1:
            removals.append(token[1:])
            continue
        if token.startswith("project:"):
            value = token.split(":", 1)[1]
            project = value or None
            continue
        if token.startswith("due:"):
            value = token.split(":", 1)[1]
            due = value or None
            continue
        if token.startswith("wait:"):
            value = token.split(":", 1)[1]
            wait = value or None
            continue

    addition_set = set(additions)
    removal_set = set(removals)
    collision = addition_set & removal_set
    addition_set -= collision
    removal_set -= collision

    return UpdateDescriptor(
        index=index,
        add_tags=frozenset(addition_set),
        remove_tags=frozenset(removal_set),
        project=project,
        due=due,
        wait=wait,
    )
