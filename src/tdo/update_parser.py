from __future__ import annotations

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

from .update_descriptor import UpdateDescriptor


__all__ = ["parse_update"]


_UPDATE_GRAMMAR = Grammar(
    r"""
    update = ws? segments? ws?
    segments = part (ws part)*
    part = add_tag / remove_tag / project / due / wait / word
    add_tag = "+" tagname
    remove_tag = "-" tagname
    project = "project:" value
    due = "due:" value
    wait = "wait:" value
    tagname = ~"[^ \t\r\n]+"
    value = ~"[^ \t\r\n]*"
    word = ~"[^ \t\r\n]+"
    ws = ~"[ \t\r\n]+"
    """,
)


class _UpdateVisitor(NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self._additions: list[str] = []
        self._removals: list[str] = []
        self.project: str | None = None
        self.due: str | None = None
        self.wait: str | None = None

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
