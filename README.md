# Async CalDAV TODO CLI

An async Python-cli inspired by Taskwarrior that stores tasks on CalDAV servers while preserving X-Properties for better interoperability with clients such as Apple Calendar, Tasks.org, and Deck.

## Features

- Taskwarrior-style syntax for adding, modifying, and deleting tasks (e.g. `todo add my task pri:L due:eod`).
- Built with existing Python CalDAV libraries and async patterns wherever possible.
- Retains X-Property metadata like `X-APPLE-SORT-ORDER`, `X-TASKS-ORG-ORDER`, `X-PRIORITY-SCORE`, and other client-specific extensions so downstream clients keep custom ordering, workflows, and tracking data.
- Designed for extensibility (checklists, Kanban states, time tracking metadata, Nextcloud Deck links) by mirroring Taskwarrior semantics over CalDAV.

## Getting Started

1. Install dependencies (see `pyproject.toml`).
2. Configure CalDAV credentials (URL, username, password or token) in `~/.config/todo/config.<env>`.
3. Run `todo --help` to view available commands and syntax templates.

## Command Syntax

| Command | Description |
| --- | --- |
| `todo add <description> [pri:<level>] [due:<when>] [x:<property>:<value>]` | Add a new task with optional priority, due date, or custom X-properties. |
| `todo modify <id> [pri:<level>] [due:<when>]` | Update an existing task, e.g. `todo modify 3 pri:H`. |
| `todo del <id[,id...]>` | Delete one or more tasks (comma-separated IDs). |
| `todo list [filter]` | Show synced tasks; filters accept Taskwarrior-like tokens (`status:pending`, `pri:H`, etc.). |

### Examples

```
todo add "Refactor sync layer" pri:H due:tue x:X-APPLE-SORT-ORDER:10
todo modify 3 pri:M x:X-KANBAN-STATE:in-progress
todo del 2,5
```

## X-Property Support

The tool preserves and exposes the following CalDAV X-properties for compatibility with clients that rely on manual ordering, Kanban columns, time tracking, and workflow metadata:

- `X-APPLE-SORT-ORDER`
- `X-TASKS-ORG-ORDER`
- `X-NEXTCLOUD-DECK`
- `X-PRIORITY-SCORE`
- `X-TASK-TYPE`
- `X-TIME-TRACKING`
- `X-KANBAN-STATE`
- `X-WORKFLOW-ORDER`

Custom extensions can be added via `x:<property>:<value>` when creating or modifying tasks.

## Testing

Run the async test suite once implemented (e.g., `pytest tests`).

## Contributing

- Follow existing async patterns and prefer `asyncio`-friendly CalDAV clients.
- Preserve all known X-properties when syncing back to the CalDAV server (PUT/DELETE operations must not strip them).
- Add tests that exercise Taskwarrior-like syntax parsing, CalDAV sync flows, and X-property round-trip fidelity.
