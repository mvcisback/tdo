from __future__ import annotations

import random

from todo.update_linear_parser import parse_update as parse_update_linear
from todo.update_parser import UpdateDescriptor, parse_update as parse_update_grammar

parse_update = parse_update_linear


def test_parse_modify_input() -> None:
    descriptor = parse_update("4 -tag3 +email pri:")
    assert descriptor.index == 4
    assert descriptor.add_tags == frozenset({"email"})
    assert descriptor.remove_tags == frozenset({"tag3"})
    assert descriptor.project is None
    assert descriptor.due is None


def test_parse_add_input_with_description() -> None:
    descriptor = parse_update("my task description +tag1 +tag2 project:work due:eod")
    assert descriptor.index is None
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


def _generate_fuzz_inputs(seed: int = 0) -> list[str]:
    rng = random.Random(seed)
    samples: list[str] = []
    for _ in range(256):
        tokens: list[str] = []
        if rng.random() < 0.35:
            tokens.append(str(rng.randint(1, 99)))
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
        "3 +alpha -beta project:home due:2025-12-01",
        UpdateDescriptor(
            index=3,
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
            index=None,
            add_tags=frozenset({"food"}),
            remove_tags=frozenset({"junk"}),
            project=None,
            due=None,
            wait=None,
        ),
    ),
    (
        "12 +urgent +urgent -urgent due:tomorrow",
        UpdateDescriptor(
            index=12,
            add_tags=frozenset(),
            remove_tags=frozenset(),
            project=None,
            due="tomorrow",
            wait=None,
        ),
    ),
    (
        "42 -old +new +alpha project:work status:done",
        UpdateDescriptor(
            index=42,
            add_tags=frozenset({"new", "alpha"}),
            remove_tags=frozenset({"old"}),
            project="work",
            due=None,
            wait=None,
        ),
    ),
    (
        "due:eod +tag -tag review",
        UpdateDescriptor(
            index=None,
            add_tags=frozenset(),
            remove_tags=frozenset(),
            project=None,
            due="eod",
            wait=None,
        ),
    ),
    (
        "4 wait:2d +alpha -beta",
        UpdateDescriptor(
            index=4,
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
