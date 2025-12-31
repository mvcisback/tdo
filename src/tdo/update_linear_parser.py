from __future__ import annotations

from .update_descriptor import UpdateDescriptor

__all__ = ["parse_update"]


def _parse_priority(raw: str) -> int | None:
    if not raw:
        return None
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


def parse_update(raw: str) -> UpdateDescriptor:
    tokens = [segment for segment in raw.strip().split() if segment]
    description_parts: list[str] = []
    additions: list[str] = []
    removals: list[str] = []
    project: str | None = None
    due: str | None = None
    wait: str | None = None
    priority: int | None = None
    status: str | None = None
    summary: str | None = None
    x_properties: dict[str, str] = {}

    for token in tokens:
        # Tags
        if token.startswith("+") and len(token) > 1:
            additions.append(token[1:])
            continue
        if token.startswith("-") and len(token) > 1:
            removals.append(token[1:])
            continue
        
        # Key-value metadata
        if ":" in token:
            key, rest = token.split(":", 1)
            key_lower = key.strip().lower()
            value = rest.strip()
            
            if key_lower == "project":
                project = value  # Keep empty string to signal "unset"
                continue
            if key_lower == "due":
                due = value  # Keep empty string to signal "unset"
                continue
            if key_lower == "wait":
                wait = value  # Keep empty string to signal "unset"
                continue
            if key_lower == "pri":
                if not value:
                    priority = 0  # Use 0 to signal "unset"
                else:
                    parsed_priority = _parse_priority(value)
                    if parsed_priority is not None:
                        priority = parsed_priority
                continue
            if key_lower == "status":
                status = value.upper() if value else None
                continue
            if key_lower == "summary":
                summary = rest
                continue
            if key_lower == "x" and ":" in rest:
                prop_key, prop_value = rest.split(":", 1)
                x_properties[prop_key] = prop_value
                continue
        
        # Description word
        description_parts.append(token)

    addition_set = set(additions)
    removal_set = set(removals)
    collision = addition_set & removal_set
    addition_set -= collision
    removal_set -= collision

    description = " ".join(part for part in description_parts if part.strip())

    return UpdateDescriptor(
        description=description,
        add_tags=frozenset(addition_set),
        remove_tags=frozenset(removal_set),
        project=project,
        due=due,
        wait=wait,
        priority=priority,
        status=status,
        summary=summary,
        x_properties=x_properties,
    )
