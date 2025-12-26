# TODO

1. **Define CLI entrypoint** – build an async Taskwarrior-style parser that understands `todo add`, `todo modify`, and `todo del` commands with Taskwarrior-style modifiers and comma-separated IDs for deletes.
2. **CalDAV integration layer** – choose an async-friendly Python CalDAV client, wire authentication/collection discovery, and ensure PUT/DELETE round-trips preserve all known X-properties.
3. **X-property management** – add helpers to read/write `X-APPLE-SORT-ORDER`, `X-TASKS-ORG-ORDER`, `X-PRIORITY-SCORE`, `X-TASK-TYPE`, `X-TIME-TRACKING`, `X-KANBAN-STATE`, `X-WORKFLOW-ORDER`, and `X-NEXTCLOUD-DECK` from task metadata.
4. **Local cache + sync** – maintain a lightweight cache of tasks so the CLI can operate offline, then reconcile with the CalDAV backend asynchronously.
5. **Testing harness** – write pytest scenarios for command parsing, CalDAV interactions (mocked), and X-property preservation.
6. **Documentation** – extend `README.md` with configuration examples, `examples/` folder, and developer notes once core functionality stabilizes.
