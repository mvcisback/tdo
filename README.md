# Async CalDAV TODO CLI

An async Python-cli inspired by Taskwarrior that stores tasks on CalDAV servers while preserving X-Properties for better interoperability with clients such as Apple Calendar, Tasks.org, and Deck.

## Features

- Taskwarrior-style syntax for adding, modifying, and deleting tasks (e.g. `todo add my task pri:L due:eod`).
- Built with existing Python CalDAV libraries and async patterns wherever possible.
- Retains X-Property metadata like `X-APPLE-SORT-ORDER`, `X-TASKS-ORG-ORDER`, `X-PRIORITY-SCORE`, and other client-specific extensions so downstream clients keep custom ordering, workflows, and tracking data.
- Designed for extensibility (checklists, Kanban states, time tracking metadata, Nextcloud Deck links) by mirroring Taskwarrior semantics over CalDAV.

## Getting Started

1. Install dependencies (see `pyproject.toml`).
2. Configure CalDAV credentials (URL, username, password or token) in a TOML file under `~/.config/todo/config.<env>.toml` using the `[caldav]` section.
3. Use `todo config init` to interactively build that file (see options below) and persist the credentials you just collected.
4. Run `todo --help` to view available commands and syntax templates.

### Configuration helper

`todo config init` prompts for the CalDAV calendar URL and username, then stores them under `~/.config/todo/config.<env>.toml` (defaults to `default`).

Pass `--env <name>` to target a different environment (e.g. `default`, `personal`, `work`), `--config-home` to redirect the base directory, and `--force` to overwrite an existing file. Additional options such as `--password`, `--token`, and `--calendar-url` / `--username` can be used to skip the interactive prompts when automating the setup. You can also skip `todo config init` entirely by passing `--config-file <path>` to the other commands so they load the given TOML directly.

Sample configs live in `examples/configs/` to show how to write valid `[caldav]` sections for each environment.

### Radicale test server

Launch a disposable CalDAV backend via `nix run .#radicaleTest`; it binds to port `5232` and exposes a single user `test` with password `test`. The server is backed by the embedded Radicale config and resets every time the script exits.

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

Run the async test suite inside the registered `.venv` so dependencies stay locked:

```
nix develop --command bash -c "uv run pytest"
```

## Contributing

- Follow existing async patterns and prefer `asyncio`-friendly CalDAV clients.
- Preserve all known X-properties when syncing back to the CalDAV server (PUT/DELETE operations must not strip them).
- Add tests that exercise Taskwarrior-like syntax parsing, CalDAV sync flows, and X-property round-trip fidelity.
