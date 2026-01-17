## 0.10.0 (2026-01-17)

### Feat

- **completions**: Improve fish autocomplete behavior
- **completions**: Add project and tag filter completions
- **cli**: Add fish shell autocompletion
- **cli**: Add move command to transfer tasks between environments
- **cli**: Reverse list order with highest priority at bottom

### Fix

- **time-parser**: Make eom/eoy/eoq/eow mean end of current period
- **cache**: Prevent race condition in task index assignment

## 0.9.0 (2026-01-02)

### Feat

- **cli**: Add interactive prioritize command
- **caldav**: Add URL and ATTACH property support
- **cache**: Add UTC-normalized date columns for SQL-based filtering
- **cli**: Add waiting command and hide future-wait tasks from list

## 0.8.0 (2026-01-01)

### Feat

- **cli**: Add start and stop commands for task status transitions
- Seperate the listed tasks by started and backlog.
- Change default action state to NEEDS-ACTION
- **cli**: Add undo command to revert last transaction
- **cache**: Add transaction log for TaskSetDiff operations

### Refactor

- **cache**: Split tasks into three tables for cleaner state management
- ruff check --fix

## 0.7.0 (2025-12-31)

### Feat

- **cli**: Display structured change summaries for all commands

### Refactor

- Add compositional TaskDiff and TaskSetDiff for sync operations
- Make TaskData generic over time field types
- **models**: Use composition for Task with nested TaskData

## 0.6.0 (2025-12-31)

### Feat

- **cli**: Add metadata-based filtering for list command
- **cli**: Make add, modify, delete, pull outputs more verbose
- **cli**: Add --version option

## 0.5.0 (2025-12-30)

### Feat

- Allow unsetting fields with empty values
- Add show command for detailed task view

### Refactor

- Use DTSTART instead of X-WAIT for wait functionality
- Unify date/duration parsing for due and wait

## 0.4.0 (2025-12-30)

### Feat

- Implement stable task indices
- **ui**: back background grey on even rows.
- **ui**: Relax input syntax to allow interspersing tags and tokens.

### Fix

- Move --env to main parser to avoid issues.
- caches are scoped by env now.
- Scope keyring service to the env variable.
- Robustly set keyring name.

### Refactor

- Combine description, metadata, and other parsing logic.
- Remove unused index in update descriptor.

### Perf

- Switch to async interfaces, particularly for sqlite.

## 0.3.0 (2025-12-28)

### Feat

- Support keyring in order to avoid hardcoding password in config.

## 0.2.0 (2025-12-28)

### Feat

- **ui**: CLI now dynamically sizes table.
- Switch to using an sqlite backed with push/pull to caldav.
- Use built in categories for tags.
- Change ui to use filters first per taskwarrior.
- **cli**: support Taskwarrior tag syntax
- Implement done command.
- **ui**: colorizing outputs.
- **cli**: allow indexing tasks via table view
- **ui**: Colorization and prettification of list view.
- **ui**: surpress UID.
- **nix**: Add disposable radicale server for testing.
- switch to toml configs and caldav lib
- sketch CLI entry points for MVP

### Fix

- Multi word descriptions not properly tokenized.
- done tasks no longer list and -tag now works.
- Switch to direct argpase + PEG based parser to handle -tags.

### Refactor

- Implement time parsing based on taskwarrior
- Add wait to the new parser.
- **parser**: Start working on new grammar for CLI.
- Use rich tables.

### Perf

- Switch to lazy loading caldav dependencies.
- Removing the dependence on caldav client for non-sync commands.
- Switch to linear parser.
- Combining caldav calls for performance.
