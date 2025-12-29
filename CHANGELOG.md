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
