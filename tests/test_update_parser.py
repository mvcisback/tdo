from __future__ import annotations

import random

from tdo.update_linear_parser import parse_update as parse_update_linear
from tdo.update_descriptor import UpdateDescriptor
from tdo.update_parser import parse_update as parse_update_grammar

parse_update = parse_update_linear


def test_parse_modify_input() -> None:
    descriptor = parse_update("-tag3 +email pri:")
    assert descriptor.add_tags == frozenset({"email"})
    assert descriptor.remove_tags == frozenset({"tag3"})
    assert descriptor.project is None
    assert descriptor.due is None


def test_parse_add_input_with_description() -> None:
    descriptor = parse_update("my task description +tag1 +tag2 project:work due:eod")
    assert descriptor.add_tags == frozenset({"tag1", "tag2"})
    assert descriptor.remove_tags == frozenset()
    assert descriptor.project == "work"
    assert descriptor.due == "eod"


def test_tags_present_in_both_sets_are_dropped() -> None:
    descriptor = parse_update("+shared -shared +keep -remove")
    assert descriptor.add_tags == frozenset({"keep"})
    assert descriptor.remove_tags == frozenset({"remove"})


def test_blank_project_and_due_resolve_to_none() -> None:
    descriptor = parse_update("project: due:")
    assert descriptor.project is None
    assert descriptor.due is None


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
    project: str | None = None
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
                project = value or None
                continue
            if key_lower == "due":
                due = value or None
                continue
            if key_lower == "wait":
                wait = value or None
                continue
            if key_lower == "pri":
                try:
                    priority = int(value) if value else None
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


def _generate_fuzz_inputs(seed: int = 0) -> list[str]:
    rng = random.Random(seed)
    samples: list[str] = []
    for _ in range(256):
        tokens: list[str] = []
        for _ in range(rng.randint(0, 6)):
            tokens.append(rng.choice(_TOKEN_OPTIONS))
        samples.append(" ".join(tokens))
    return samples


def test_parse_update_matches_simulation_for_random_inputs() -> None:
    for raw in _generate_fuzz_inputs(seed=0):
        expected = _simulate_descriptor(raw)
        assert parse_update(raw) == expected


def test_linear_parser_matches_grammar_for_random_inputs() -> None:
    for raw in _generate_fuzz_inputs(seed=1):
        assert parse_update_linear(raw) == parse_update_grammar(raw)


_EXAMPLE_INPUTS: list[tuple[str, UpdateDescriptor]] = [
    (
        "+alpha -beta project:home due:2025-12-01",
        UpdateDescriptor(
            description="",
            add_tags=frozenset({"alpha"}),
            remove_tags=frozenset({"beta"}),
            project="home",
            due="2025-12-01",
            wait=None,
        ),
    ),
    (
        "grocery list +food -junk project:",
        UpdateDescriptor(
            description="grocery list",
            add_tags=frozenset({"food"}),
            remove_tags=frozenset({"junk"}),
            project=None,
            due=None,
            wait=None,
        ),
    ),
    (
        "+urgent +urgent -urgent due:tomorrow",
        UpdateDescriptor(
            description="",
            add_tags=frozenset(),
            remove_tags=frozenset(),
            project=None,
            due="tomorrow",
            wait=None,
        ),
    ),
    (
        "-old +new +alpha project:work status:done",
        UpdateDescriptor(
            description="",
            add_tags=frozenset({"new", "alpha"}),
            remove_tags=frozenset({"old"}),
            project="work",
            due=None,
            wait=None,
            status="DONE",
        ),
    ),
    (
        "due:eod +tag -tag review",
        UpdateDescriptor(
            description="review",
            add_tags=frozenset(),
            remove_tags=frozenset(),
            project=None,
            due="eod",
            wait=None,
        ),
    ),
    (
        "wait:2d +alpha -beta",
        UpdateDescriptor(
            description="",
            add_tags=frozenset({"alpha"}),
            remove_tags=frozenset({"beta"}),
            project=None,
            due=None,
            wait="2d",
        ),
    ),
]


def test_parse_update_examples() -> None:
    for raw, expected in _EXAMPLE_INPUTS:
        assert parse_update(raw) == expected
