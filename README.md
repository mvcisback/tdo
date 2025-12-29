# Async CalDAV TDO CLI

An async Python-cli inspired by Taskwarrior that stores tasks on CalDAV servers while preserving X-Properties for better interoperability with clients such as Apple Calendar, Tasks.org, and Deck.

## Features

- Taskwarrior-style syntax for adding, modifying, and deleting tasks (e.g. `tdo add my task pri:L due:eod`).
- Built with existing Python CalDAV libraries and async patterns wherever possible.
- Retains X-Property metadata like `X-APPLE-SORT-ORDER`, `X-TASKS-ORG-ORDER`, `X-PRIORITY-SCORE`, and other client-specific extensions so downstream clients keep custom ordering, workflows, and tracking data.
- Designed for extensibility (checklists, Kanban states, time tracking metadata, Nextcloud Deck links) by mirroring Taskwarrior semantics over CalDAV.
- Add/modify tokens are parsed via a shared PEG grammar so you can mix plain description words with `+tags`, `-tags`, `project:foo`, `due:eod`, `wait:2d`, and other taskwarrior-esque directives.

## Getting Started

1. Install dependencies (see `pyproject.toml`).
2. Configure CalDAV credentials (URL, username, password or token) in a TOML file under `~/.config/tdo/config.<env>.toml` using the `[caldav]` section.
3. Use `tdo config init` to interactively build that file (see options below) and persist the credentials you just collected.
4. Run `tdo --help` to view available commands and syntax templates.

### Configuration helper

`tdo config init` prompts for the CalDAV calendar URL and username, then stores them under `~/.config/tdo/config.<env>.toml` (defaults to `default`).

Pass `--env <name>` to target a different environment (e.g. `default`, `personal`, `work`), `--config-home` to redirect the base directory, and `--force` to overwrite an existing file. Additional options such as `--password`, `--token`, and `--calendar-url` / `--username` can be used to skip the interactive prompts when automating the setup.

### Environment-based configuration

Set `TDO_CONFIG_FILE` to point at any existing TOML (or ini-style) CalDAV configuration file when the defaults in `~/.config/tdo/` do not work for you. This takes precedence over other config file discovery so scripts can switch contexts quickly by exporting the variable before invoking `tdo`. All commands honor that environment variable automatically (no need for `--config-file`).

Sample configs live in `examples/configs/` to show how to write valid `[caldav]` sections for each environment.

### Radicale test server

Launch a disposable CalDAV backend via `nix run .#radicaleTest`; it binds to port `5232` and exposes a single user `test` with password `test`. The server is backed by the embedded Radicale config and resets every time the script exits.

## Command Syntax

| Command | Description |
| --- | --- |
| `tdo add <description> [pri:<level>] [due:<when>] [x:<property>:<value>]` | Add a new task with optional priority, due date, or custom X-properties. |
| `tdo modify <id> [pri:<level>] [due:<when>]` | Update an existing task, e.g. `tdo modify 3 pri:H`. |
| `tdo del <id[,id...]>` | Delete one or more tasks (comma-separated IDs). |
| `tdo list [filter]` | Show synced tasks; filters accept Taskwarrior-like tokens (`status:pending`, `pri:H`, etc.). |

### Examples

```
tdo add "Refactor sync layer" pri:H due:tue x:X-APPLE-SORT-ORDER:10
tdo modify 3 pri:M +in-progress -backlog project:work wait:2h due:2025-08-01T09:00
tdo del 2,5
```

Add/modify commands share a PEG-driven parser that supports the Taskwarrior-style directives shown above, including `+tag`, `-tag`, `project:`, `due:`, `wait:`, and the rich date/duration expressions that mirror Taskwarrior's semantics (e.g., `tomorrow`, `eod`, `1st`, or ISO durations).
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
