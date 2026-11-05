# Dirwalker

Dirwalker is a command-line filesystem inventory and duplicate-directory analysis
tool backed by MariaDB/MySQL. It queues scan jobs, records directory metadata,
resumes work from checkpoints, checks for changes, and verifies duplicate
candidates with byte comparisons. Terminal and HTML reports show the results.

The current application version is **0.25.2**. Commands run from the repository
directory using `dirwalker.py`; `version25.py` is the corresponding release source.
On a deployed server, the entry point may be named `serverfiles.py`.

The tool is under development. It is not stable.

## Requirements

- Python 3.8, as specified in `.python-version` and `pyproject.toml`.
- A MariaDB/MySQL server and credentials with permission to create and manage
  the application database and tables.
- MySQL Connector/Python 8.4.0.
- A Unix-like system for scan workers, which use POSIX file locking.

## Setup

Create a virtual environment and install the runtime dependency:

```sh
python3.8 -m venv .venv
source .venv/bin/activate
python -m pip install mysql-connector-python==8.4.0
```

`requirements.txt` is a broader environment snapshot; the runtime dependency
declared in `pyproject.toml` is sufficient for the command-line application.

Copy the example database settings to a private local file:

```sh
mkdir -p .secrets
cp doc/shellenv_example.sh .secrets/shellenv
chmod 600 .secrets/shellenv
```

Edit `.secrets/shellenv` with your database settings, then load it:

```sh
source .secrets/shellenv
```

| Variable | Purpose | Default |
| --- | --- | --- |
| `DB_HOST` | Database hostname | `localhost` |
| `DB_PORT` | Database port | `3306` |
| `DB_NAME` | Application database | `filestatus` |
| `DB_USER` | Database username | Required |
| `DB_PASSWORD` | Database password | Required |
| `DB_IO_TIMEOUT` | Database I/O timeout in seconds | `30` |

The example settings use the database name `dirwalker`. Keep credentials out
of version control. Database timestamps use UTC.

For a new installation, initialize and check the schema:

```sh
python dirwalker.py init
python dirwalker.py check
```

On an existing installation, `init` can offer to rebuild the inventory and
clear related job data. Stop workers and back up the database before schema
changes. Use the applicable migrations in [sql/](sql/) for upgrades; see the
[database documentation](doc/database_tbls.md).

## Scan directories

For paths accessible on the scan server (host ID 0), queue a scan and start
the worker:

```sh
python dirwalker.py --jobs-insert /path/to/files '.git,.venv,cache'
python dirwalker.py server
```

The worker stays running to process queued jobs. In another terminal with the
same environment, inspect progress:

```sh
python dirwalker.py show jobs
python dirwalker.py --jobs-list-active
python dirwalker.py --dirs-summary-data
```

Press Ctrl-C to stop the worker gracefully. Checkpoints allow interrupted work
to resume. Use `--jobs-insert-scanall` in place of `--jobs-insert` to scan every
subtree instead of skipping recently completed subtrees.

To scan another computer, register it and use its assigned positive host ID:

```sh
python dirwalker.py --setmyhostid
python dirwalker.py show hosts
python dirwalker.py --hostid 1 --jobs-insert /path/on/local/computer
python dirwalker.py localserver --hostid 1
```

Replace `1` with the actual registered ID. Each worker must be able to read
the paths assigned to its host.

## Verify duplicates

After the relevant directory scans finish:

```sh
python dirwalker.py --doubles-list
python dirwalker.py --doubles-letcheck
```

Record a job ID printed by `--doubles-letcheck`. Run `serverplus` for host 0,
or `localserver --hostid ID` on the appropriate registered computers, to process
the comparison jobs. Ordinary `server` does not process byte comparisons.

```sh
python dirwalker.py serverplus
```

In another terminal, monitor the batch and generate its HTML report:

```sh
python dirwalker.py --doubles-progress JOBID
python dirwalker.py --doubles-tree-html JOBID
```

Replace `JOBID` with the recorded number. Wait for the batch's comparison jobs
to finish for a complete report. The report is written as
`doubles-tree-JOBID.html` in the current directory.

## Documentation

```sh
python dirwalker.py --help
python dirwalker.py --doubles-help
```

Detailed documentation is in German:

- [Documentation index](doc/README.md)
- [Setup and everyday workflows](doc/workflow.md)
- [Jobs, scan stages, and worker behavior](doc/modus.md)
- [Change detection and rescanning](doc/rescan.md)
- [Duplicate verification](doc/duplikate-pruefen.md)
- [Database troubleshooting](doc/database_solutions.md)

## Development

Install pytest in the active environment, then run the offline checks:

```sh
python -m pip install 'pytest<8.4'
make test-dryrun
make test
```

The pytest version above supports Python 3.8. The Makefile uses
`.venv/bin/python` by default; override it with `make test PYTHON=/path/to/python`.
`make test-dryrun` checks release consistency and module separation.
`make test` runs the test suite; real database integration is enabled separately
with `make test-integration`, using credentials from `.secrets/shellenv`.

Read [the development conventions](doc/skills.md) before changing Python code.
`make install` runs release checks and then deploys via rsync/SSH using the
Makefile's `REMOTE` and `REMOTEDIR` settings.

## License

Licensed under the [MIT License](LICENSE).
