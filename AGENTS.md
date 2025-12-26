# AGENTS GUIDE

## High-level overview
- Lightweight CLI inspired by Taskwarrior semantics that stores async TODOs in CalDAV calendars.
- `main.py` currently hosts the entry point; expand with actual CalDAV sync logic and command handling.
- Serialization and task persistence happen through X-Properties to keep metadata for clients such as Apple Calendar, Tasks.org, and Deck.

## Essential commands
| Purpose | Command |
| --- | --- |
| Enter the nix development shell | `nix develop` (uses the repository's `flake.nix`). |
| Run tests inside the pinned virtualenv | `nix develop --command bash -c "uv run pytest"` |
| Install deps / update uv.lock | `nix develop --command bash -c "uv add <package>"` (or `uv add --dev` for dev deps) |
| Run the CLI | `nix develop --command bash -c "uv run python main.py"` once CLI logic is implemented. |

## Testing approach
- Tests live under `tests/`; the suite currently only contains a placeholder that asserts `True` so CI can verify basic wiring.
- Always run `uv run pytest` from within the Nix dev shell so hermetic `.venv` dependencies provided by `uv` are used.

## Nix dev shell
- `flake.nix` wires up `pyproject-nix`, `uv2nix`, and `uv` so the shell exposes `uvicorn` (when added) plus the CLI virtualenv.
- `nix develop` also exports `PYTHONPATH` to the repo root so scripts can `import` modules without extra path hacks.

## Commit conventions
- Follow Commitizen's `cz_conventional_commits` style defined in `pyproject.toml`.
- Run `uv cz commit` (inside the nix shell) to ensure the commit message template and hooks are honored.

## Notes for agents
- Refer to `README.md` for project goals, feature ideas, and CalDAV-specific details before adding functionality.
- Keep changes limited to defensive or compliant code; avoid anything that could be misused maliciously.
