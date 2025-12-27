from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


__all__ = ["UpdateDescriptor", "parse_update"]


_UPDATE_GRAMMAR = Grammar(
    r"""
    update = ws? index_part? ws? segments? ws?
    index_part = index
    segments = part (ws part)*
    part = add_tag / remove_tag / project / due / wait / word
    add_tag = "+" tagname
    remove_tag = "-" tagname
    project = "project:" value
    due = "due:" value
    wait = "wait:" value
    tagname = ~"[^\s]+"
    value = ~"[^\s]*"
    word = ~"[^\s]+"
    index = ~"[0-9]+"
    ws = ~"\s+"
    """,
)


@dataclass(frozen=True)
class UpdateDescriptor:
    index: int | None
    add_tags: FrozenSet[str]
    remove_tags: FrozenSet[str]
    project: str | None
    due: str | None
    wait: str | None


class _UpdateVisitor(NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self.index: int | None = None
        self._additions: list[str] = []
        self._removals: list[str] = []
        self.project: str | None = None
        self.due: str | None = None
        self.wait: str | None = None

    def visit_index(self, node, _visited_children):
        if self.index is None and node.text:
            self.index = int(node.text)
        return None

    def visit_add_tag(self, _node, visited_children):
        _, tag = visited_children
        if tag:
            self._additions.append(tag)
        return None

    def visit_remove_tag(self, _node, visited_children):
        _, tag = visited_children
        if tag:
            self._removals.append(tag)
        return None

    def visit_project(self, _node, visited_children):
        _, value = visited_children
        self.project = value or None
        return None

    def visit_due(self, _node, visited_children):
        _, value = visited_children
        self.due = value or None
        return None

    def visit_wait(self, _node, visited_children):
        _, value = visited_children
        self.wait = value or None
        return None

    def visit_tagname(self, node, _visited_children):
        return node.text

    def visit_value(self, node, _visited_children):
        return node.text

    def visit_update(self, _node, _visited_children):
        additions = set(self._additions)
        removals = set(self._removals)
        collision = additions & removals
        if collision:
            additions -= collision
            removals -= collision
        return UpdateDescriptor(
            index=self.index,
            add_tags=frozenset(additions),
            remove_tags=frozenset(removals),
            project=self.project,
            due=self.due,
            wait=self.wait,
        )

    def generic_visit(self, node, visited_children):
        return visited_children or node.text


def parse_update(raw: str) -> UpdateDescriptor:
    visitor = _UpdateVisitor()
    tree = _UPDATE_GRAMMAR.parse(raw or "")
    return visitor.visit(tree)
