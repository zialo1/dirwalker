# Python development conventions for filewalker

Apply these conventions whenever adding or changing Python code in this project. Keep existing behavior and naming conventions unless the request says otherwise.

## File structure and documentation

- Start scripts with a Python 3 shebang and UTF-8 encoding declaration.
- Put a comprehensive module docstring before imports. Explain the script's purpose, commands and modes, scan behavior, environment variables, dependencies, and how to run its tests.
- Keep command-line help and other user-facing messages in German, consistent with the existing filewalker commands.
- Use type-appropriate names and short docstrings for public classes and methods. Keep comments focused on decisions that are not obvious from the code.

## Database access

- Keep database settings and connection creation in `DataBaseConnectionCore` / `DatabaseConnection`. Put all table definitions, table-name constants, constraints, structural SQL checks, and SQL statement-timeout helpers in `sqlstructure.py` (`SQLStructure` and `SQLStructureCLI`). `DataBaseCLI` exposes the administration API through this mixin. Transport/socket timeouts remain with connection management.
- Read `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` into instance attributes. Do not keep parallel module-level `DB_*` configuration constants.
- Keep table identifiers as class variables on `SQLStructure`, inherited by the database provider.
- Create one module-level `database_connection = DatabaseConnection()` and reuse it as the provider throughout the script.
- `DatabaseConnection.connect()` must return a fresh connector handle on every call. Use it with a context manager:

  ```python
  with database_connection.connect() as connection:
      cursor = connection.cursor()
      try:
          # Execute parameterized SQL and commit on success.
          ...
      except Exception:
          connection.rollback()
          raise
      finally:
          cursor.close()
  ```

- Use `connect_server()` only for operations that need a connection without a selected database, such as creating the database itself.
- Use `database_connection.utc_now` as a property, without parentheses. It returns a naive UTC `datetime` suitable for MariaDB `DATETIME` columns.
- Prefer `mysql-connector-python` for this project so installation does not require a Synology C compiler; retain MariaDB Connector/Python as a fallback if already available. Use UTF-8 (`utf8mb4`), parameterized SQL, and the connector's supported `%s` placeholders. Never interpolate user data into SQL.
- Keep schema initialization in `SQLStructureCLI` and destructive job cleanup in the database administration layer; client dispatch calls these methods. Do not add module-level migration functions or automatic migrations of existing tables.

## Classes and command-line handling

- Keep database behavior in `DatabaseConnection`, client command dispatch in `client_operations`, and server lifecycle and scanning orchestration in `server_operations`.
- Put signal handling, polling, `run_spawn()`, and the static `_child_entry()` process target in `server_operations`.
- Keep `_child_entry()` static and pickleable so Python's `spawn` multiprocessing mode can call it.
- Use the existing `ClientArgumentParser` for client options and server subcommands. Administrative options use `--` names; `check`, `init`, `serverinit`, `server`, `serverplus`, `localserver`, and `show` are subcommands. Dispatch through the relevant operations class.
- Preserve automatic server startup when the script is run without arguments under the supported server entry-point filename (see `ApplicationContext` and parser tests).

## Tests

- Keep current tests in the seven `test_<production-module>.py` files listed in `doc/README.md`. Add feature coverage to its owning module test file instead of creating separate feature or historical-version test files. Release and structural checks belong in `test_sqlstructure.py`.

- Add `unittest` coverage for the script and each production class when adding or changing behavior.
- Keep fast unit tests independent of a live database by mocking the connector, signal handlers, child processes, and filesystem-dependent operations as needed.
- Keep real database checks as clearly identified integration tests. They should verify the database and required columns, use a unique test record, check every written field, and always remove the record in cleanup code.
- Run the focused tests after changes. Do not make routine tests depend on deleting or modifying existing user data.
