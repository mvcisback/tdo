from __future__ import annotations

import random

from tdo.models import TaskData
from tdo.update_linear_parser import parse_update as parse_update_linear
from tdo.update_descriptor import UpdateDescriptor
from tdo.update_parser import parse_update as parse_update_grammar

parse_update = parse_update_linear


def test_parse_modify_input() -> None:
    descriptor = parse_update("-tag3 +email pri:")
    assert descriptor.add_data.categories == ["email"]
    assert descriptor.remove_data.categories == ["tag3"]
    assert descriptor.add_data.x_properties.get("X-PROJECT") is None
    assert descriptor.add_data.due is None


def test_parse_add_input_with_description() -> None:
    descriptor = parse_update("my task description +tag1 +tag2 project:work due:eod")
    assert set(descriptor.add_data.categories or []) == {"tag1", "tag2"}
    assert descriptor.remove_data.categories is None
    assert descriptor.add_data.x_properties.get("X-PROJECT") == "work"
    assert descriptor.add_data.due == "eod"


def test_tags_present_in_both_sets_are_dropped() -> None:
    descriptor = parse_update("+shared -shared +keep -remove")
    assert set(descriptor.add_data.categories or []) == {"keep"}
    assert set(descriptor.remove_data.categories or []) == {"remove"}


def test_blank_project_and_due_resolve_to_empty_string() -> None:
    """Empty values signal 'unset' and are preserved as empty strings."""
    descriptor = parse_update("project: due:")
    assert descriptor.add_data.x_properties.get("X-PROJECT") == ""
    assert descriptor.add_data.due == ""


_TOKEN_OPTIONS = [
    "+alpha",
    "+beta",
    "+shared",
    "+keep",
    "-alpha",
    "-beta",
    "-shared",
    "-remove",
    "project:work",
    "project:home",
    "project:",
    "due:eod",
    "due:tomorrow",
    "due:",
    "wait:2d",
    "wait:",
    "pri:5",
    "pri:",
    "status:done",
    "summary:check",
    "word",
    "misc",
]


def _simulate_descriptor(raw: str) -> UpdateDescriptor:
    tokens = [token for token in raw.strip().split() if token]
    description_parts: list[str] = []
    additions: list[str] = []
    removals: list[str] = []
    due: str | None = None
    wait: str | None = None
    priority: int | None = None
    status: str | None = None
    summary: str | None = None
    x_properties: dict[str, str] = {}

    for token in tokens:
        if token.startswith("+") and len(token) > 1:
            additions.append(token[1:])
            continue
        if token.startswith("-") and len(token) > 1:
            removals.append(token[1:])
            continue
        if ":" in token:
            key, rest = token.split(":", 1)
            key_lower = key.strip().lower()
            value = rest.strip()

            if key_lower == "project":
                x_properties["X-PROJECT"] = value  # Keep empty string to signal "unset"
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
                    try:
                        priority = int(value)
                    except ValueError:
                        pass
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

        description_parts.append(token)

    addition_set = set(additions)
    removal_set = set(removals)
    collision = addition_set & removal_set
    addition_set -= collision
    removal_set -= collision

    description = " ".join(part for part in description_parts if part.strip())
    final_summary = summary if summary is not None else (description if description else None)

    add_data: TaskData[str] = TaskData(
        summary=final_summary,
        status=status,
        due=due,
        wait=wait,
        priority=priority,
        x_properties=x_properties,
        categories=list(addition_set) if addition_set else None,
    )

    remove_data: TaskData[str] = TaskData(
        categories=list(removal_set) if removal_set else None,
    )

    return UpdateDescriptor(add_data=add_data, remove_data=remove_data)


def _generate_fuzz_inputs(seed: int = 0) -> list[str]:
    rng = random.Random(seed)
    samples: list[str] = []
    for _ in range(256):
        tokens: list[str] = []
        for _ in range(rng.randint(0, 6)):
            tokens.append(rng.choice(_TOKEN_OPTIONS))
        samples.append(" ".join(tokens))
    return samples


def _normalize_descriptor(d: UpdateDescriptor) -> UpdateDescriptor:
    """Normalize a descriptor for comparison by sorting categories."""
    add_cats = sorted(d.add_data.categories) if d.add_data.categories else None
    remove_cats = sorted(d.remove_data.categories) if d.remove_data.categories else None

    return UpdateDescriptor(
        add_data=TaskData(
            summary=d.add_data.summary,
            status=d.add_data.status,
            due=d.add_data.due,
            wait=d.add_data.wait,
            priority=d.add_data.priority,
            x_properties=dict(d.add_data.x_properties),
            categories=add_cats,
        ),
        remove_data=TaskData(
            categories=remove_cats,
        ),
    )


def test_parse_update_matches_simulation_for_random_inputs() -> None:
    for raw in _generate_fuzz_inputs(seed=0):
        expected = _normalize_descriptor(_simulate_descriptor(raw))
        actual = _normalize_descriptor(parse_update(raw))
        assert actual == expected, f"Mismatch for input: {raw!r}"


def test_linear_parser_matches_grammar_for_random_inputs() -> None:
    for raw in _generate_fuzz_inputs(seed=1):
        linear = _normalize_descriptor(parse_update_linear(raw))
        grammar = _normalize_descriptor(parse_update_grammar(raw))
        assert linear == grammar, f"Mismatch for input: {raw!r}"


_EXAMPLE_INPUTS: list[tuple[str, UpdateDescriptor]] = [
    (
        "+alpha -beta project:home due:2025-12-01",
        UpdateDescriptor(
            add_data=TaskData(
                summary=None,
                due="2025-12-01",
                x_properties={"X-PROJECT": "home"},
                categories=["alpha"],
            ),
            remove_data=TaskData(categories=["beta"]),
        ),
    ),
    (
        "grocery list +food -junk project:",
        UpdateDescriptor(
            add_data=TaskData(
                summary="grocery list",
                x_properties={"X-PROJECT": ""},  # Empty string signals "unset"
                categories=["food"],
            ),
            remove_data=TaskData(categories=["junk"]),
        ),
    ),
    (
        "+urgent +urgent -urgent due:tomorrow",
        UpdateDescriptor(
            add_data=TaskData(
                summary=None,
                due="tomorrow",
            ),
            remove_data=TaskData(),
        ),
    ),
    (
        "-old +new +alpha project:work status:done",
        UpdateDescriptor(
            add_data=TaskData(
                summary=None,
                status="DONE",
                x_properties={"X-PROJECT": "work"},
                categories=["new", "alpha"],
            ),
            remove_data=TaskData(categories=["old"]),
        ),
    ),
    (
        "due:eod +tag -tag review",
        UpdateDescriptor(
            add_data=TaskData(
                summary="review",
                due="eod",
            ),
            remove_data=TaskData(),
        ),
    ),
    (
        "wait:2d +alpha -beta",
        UpdateDescriptor(
            add_data=TaskData(
                summary=None,
                wait="2d",
                categories=["alpha"],
            ),
            remove_data=TaskData(categories=["beta"]),
        ),
    ),
]


def test_parse_update_examples() -> None:
    for raw, expected in _EXAMPLE_INPUTS:
        actual = _normalize_descriptor(parse_update(raw))
        expected_normalized = _normalize_descriptor(expected)
        assert actual == expected_normalized, f"Mismatch for input: {raw!r}"
