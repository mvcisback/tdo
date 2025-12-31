from __future__ import annotations

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

from .update_descriptor import UpdateDescriptor


__all__ = ["parse_update"]


_UPDATE_GRAMMAR = Grammar(
    r"""
    update = ws? segments? ws?
    segments = part (ws part)*
    part = add_tag / remove_tag / metadata / word
    add_tag = "+" tagname
    remove_tag = "-" tagname
    metadata = key ":" value
    key = ~"[a-zA-Z0-9_-]+"
    tagname = ~"[^ \t\r\n:]+"
    value = ~"[^ \t\r\n]*"
    word = ~"[^ \t\r\n:]+"
    ws = ~"[ \t\r\n]+"
    """,
)


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


class _UpdateVisitor(NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self._description_parts: list[str] = []
        self._additions: list[str] = []
        self._removals: list[str] = []
        self.project: str | None = None
        self.due: str | None = None
        self.wait: str | None = None
        self.priority: int | None = None
        self.status: str | None = None
        self.summary: str | None = None
        self.x_properties: dict[str, str] = {}

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

    def visit_metadata(self, _node, visited_children):
        key, _, value = visited_children
        if key and isinstance(key, str):
            key_lower = key.lower()
            
            if key_lower == "project":
                self.project = value  # Keep empty string to signal "unset"
            elif key_lower == "due":
                self.due = value  # Keep empty string to signal "unset"
            elif key_lower == "wait":
                self.wait = value  # Keep empty string to signal "unset"
            elif key_lower == "pri":
                if not value:
                    self.priority = 0  # Use 0 to signal "unset"
                else:
                    parsed_priority = _parse_priority(value)
                    if parsed_priority is not None:
                        self.priority = parsed_priority
            elif key_lower == "status":
                self.status = value.upper() if value else None
            elif key_lower == "summary":
                self.summary = value
            elif key_lower == "x" and value and ":" in value:
                prop_key, prop_value = value.split(":", 1)
                self.x_properties[prop_key] = prop_value
        return None

    def visit_key(self, node, _visited_children):
        return node.text

    def visit_tagname(self, node, _visited_children):
        return node.text

    def visit_value(self, node, _visited_children):
        return node.text

    def visit_word(self, node, _visited_children):
        self._description_parts.append(node.text)
        return None

    def visit_update(self, _node, _visited_children):
        additions = set(self._additions)
        removals = set(self._removals)
        collision = additions & removals
        if collision:
            additions -= collision
            removals -= collision
        
        description = " ".join(part for part in self._description_parts if part.strip())
        
        return UpdateDescriptor(
            description=description,
            add_tags=frozenset(additions),
            remove_tags=frozenset(removals),
            project=self.project,
            due=self.due,
            wait=self.wait,
            priority=self.priority,
            status=self.status,
            summary=self.summary,
            x_properties=self.x_properties,
        )

    def generic_visit(self, node, visited_children):
        return visited_children or node.text


def parse_update(raw: str) -> UpdateDescriptor:
    visitor = _UpdateVisitor()
    tree = _UPDATE_GRAMMAR.parse(raw or "")
    return visitor.visit(tree)
