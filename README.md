# Async CalDAV TDO CLI

An async Python CLI inspired by Taskwarrior semantics that stores tasks on CalDAV servers while preserving X-Properties. Tasks stay cached locally so the CLI stays fast even when the remote backend is slow, and rich metadata (sort order, Kanban states, time-tracking) is preserved for downstream clients such as Apple Calendar, Tasks.org, and Deck.

> **Warning:** This repository contains LLM-assisted code and the project is still under heavy development; APIs, CLI behavior, and storage formats may change without notice.

## Highlights

- Taskwarrior-style syntax for adding, modifying, tagging, and categorizing tasks.
- Local cache plus pull/push/sync flows so `tdo list` can show a responsive overview of your agenda without hitting the CalDAV server on every command.
- Preserves custom X-properties (`X-APPLE-SORT-ORDER`, `X-TASKS-ORG-ORDER`, `X-WORKFLOW-ORDER`, etc.) so other clients keep their metadata when TDO syncs tasks back.
- Configurable display (show UIDs, filter indices, default environment) and graceful tooling helpers for on-boarding new calendars.

## Getting Started

1. Install dependencies listed in `pyproject.toml` inside the pinned virtualenv: run `nix develop --command bash -c "uv install"` or call `uv add` whenever you need a new dependency.
2. Configure your CalDAV credentials (URL, username, and optional password or token) in `~/.config/tdo/config.<env>.toml` using `tdo config init` so the CLI can talk to your server.
3. Run `tdo pull` once to populate the local cache, then keep using `tdo list` to see your current work.
4. `tdo --help` shows the latest commands and token syntax.

### Configuration helper

`tdo config init` guides you through creating a CalDAV configuration file. It prompts for the calendar URL and username and writes them to `~/.config/tdo/config.<env>.toml` (default `env` is `default`).

Pass `--env <name>` to target a different environment (`personal`, `work`, etc.), `--config-home` to redirect the base directory, and `--force` to overwrite an existing file. Supply `--password`, `--token`, `--calendar-url`, or `--username` to skip prompts when scripting setup.

### Environment-based configuration

Set `TDO_CONFIG_FILE` to point at any existing TOML/INI CalDAV configuration when the default discovery (`~/.config/tdo/config.<env>.toml`, `TDO_ENV`, `TDO_CONFIG_HOME`) does not work for you. This variable takes precedence over all other discovery paths, so you can swap calendars by exporting `TDO_CONFIG_FILE` before each invocation without touching your global config.

Other environment variables that influence behavior:

- `TDO_ENV` specifies which `config.<env>.toml` file to load when you don’t pass `--env`.
- `TDO_SHOW_UIDS` (true/false) enables the UID column in the listing table without modifying your workflow.
- `TDO_CALDAV_URL`, `TDO_USERNAME`, `TDO_PASSWORD`, and `TDO_TOKEN` act as overrides when you don’t want to store secrets on disk.

Sample configs live in `examples/configs/` so you can copy the TOML structure for each environment.

### Radicale test server

Launch a disposable CalDAV backend via `nix run .#radicaleTest`; it binds to port `5232` and exposes a user `test`/`test`. The server resets every time the script exits.

## Command Syntax

| Command | What it does |
| --- | --- |
| `tdo add <description> [pri:<level>] [due:<when>] [x:<property>:<value>]` | Create a new task. Supply `+tags`, `-tags`, `project:`, `wait:`, and Taskwarrior-style tokens anywhere in the remainder arguments. |
| `tdo modify [filter] <tokens>` | Apply updates to one or more cached tasks. Tokens include `summary:<text>`, `pri:<level>`, `status:<value>`, X-property overrides (`x:<PROP>:<value>`), and tag manipulations (`+tag`, `-tag`). |
| `tdo do [filter]` | Mark filtered tasks as completed (`status=COMPLETED`). |
| `tdo del [filter]` | Delete filtered tasks from the CalDAV server. |
| `tdo list [filter]` | Show cached tasks (default command). Filters accept Taskwarrior-like expressions or comma-separated numeric indices (e.g. `1,3`). Completed tasks are hidden by default. |
| `tdo pull` | Download tasks from the CalDAV server into the local cache (`sqlite_cache`). |
| `tdo push` | Upload locally created/updated/deleted tasks back to the CalDAV server. |
| `tdo sync` | Pull and immediately push local changes, reporting how many tasks were fetched, created, updated, and deleted. |
| `tdo config init [options]` | Create or overwrite a CalDAV config file. |

Filtering and defaults:

- If the first argument looks like a comma-separated list of digits (`1`, `2,4`, etc.) and is not a recognized command, TDO treats it as a filter index before the actual command. That means `tdo 1 modify +tag` targets the first task in the default list.
- Leaving the command name off makes `tdo list` the default: `tdo 2` will list only the second task.
- Filter tokens are always interpreted against the cached task list sorted by due date, priority, summary, and UID.

## Examples

```
tdo add "Refactor sync layer" pri:H due:tue x:X-APPLE-SORT-ORDER:10 +backend project:work wait:2h
tdo 1,2 modify summary:Updated +in-progress -backlog
tdo 3 do
tdo pull
tdo push
tdo sync
tdo config init --force --env personal --calendar-url https://cal.example.com/work --username worker --token tok
```

## Task Listing

`tdo list` reads from the local SQLite cache (run `tdo pull` first) and hides completed tasks by default. It prints a Rich table with columns for ID, Age, Project, Tag, Due date, Description, Urgency, and optionally UID when `show_uids` is enabled in your config or via `TDO_SHOW_UIDS`. Use `tdo list --help` or `tdo --help` for the latest column and pagination knobs.

## X-Property Support

TDO preserves and exposes the following CalDAV X-properties so downstream clients retain manual ordering, Kanban states, time tracking, and workflow metadata:

- `X-APPLE-SORT-ORDER`
- `X-TASKS-ORG-ORDER`
- `X-NEXTCLOUD-DECK`
- `X-PRIORITY-SCORE`
- `X-TASK-TYPE`
- `X-TIME-TRACKING`
- `X-KANBAN-STATE`
- `X-WORKFLOW-ORDER`

Custom extensions can still be set via `x:<property>:<value>` on the `add` and `modify` commands.

## Testing

Run the async test suite inside the registered `.venv` so dependencies stay locked:

```
nix develop --command bash -c "uv run pytest"
```

## Contributing

- Prefer asyncio-friendly CalDAV clients and preserve every X-property when syncing (PUT/DELETE operations must not strip them).
- Add tests for the Taskwarrior-style parser, CalDAV sync flows, and SQLite cache behavior.
- Keep the CLI feedback fast, clear, and consistent with the column layout described above.
