PYTHON ?= .venv/bin/python
ENV_FILE ?= ./.secrets/shellenv
REMOTE ?= synol
REMOTEDIR ?= prog/serverfiles

LATEST_VERSION := $(shell ls version*.py 2>/dev/null | sed -n 's/^version\([0-9][0-9]*\)\.py$$/\1/p' | sort -n | tail -1)
LATEST_FILE := version$(LATEST_VERSION).py

.DEFAULT_GOAL := test
.PHONY: install serverenv tests test test-dryrun test-integration test-versions \
        test-separation-of-modules

test tests:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m pytest -q test_*.py

# Offline gate: versions -> module isolation -> install.
# Real database integration is an explicit, separate target.
# This ordering also holds when make is invoked with -j.
test-dryrun:
	@echo "Teste auf Server/Usermodultrennung und auf Version $(LATEST_VERSION)"
	@$(MAKE) --no-print-directory test-separation-of-modules

test-versions:
	@PYTHONDONTWRITEBYTECODE=1 RELEASE_SOURCE="$(LATEST_FILE)" $(PYTHON) -m pytest -q test_sqlstructure.py

test-separation-of-modules: test-versions
	@PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m pytest -q test_server_operations.py::TestServerModuleLoading

test-integration: test-separation-of-modules
	@test -f "$(ENV_FILE)" || { echo "Environment file missing: $(ENV_FILE)" >&2; exit 1; }
	@set -a; . "$(ENV_FILE)"; set +a; \
		test -n "$$DB_USER" && test "$${DB_PASSWORD+x}" = x || { echo 'DB_USER and DB_PASSWORD must be configured.' >&2; exit 1; }; \
		DIRWALKER_DB_INTEGRATION=1 PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m pytest \
			test_DataBaseCLI.py::TestHostidOneDatabaseIntegration -v

# Explicit environment export remains available separately from installation.
serverenv: _serverenv

_serverenv: $(ENV_FILE)
	@umask 077; sed 's/^export DB_HOST=.*/export DB_HOST=localhost/' "$(ENV_FILE)" > "$@"

# No local deployment copy, environment export or network command before tests pass.
install: test-dryrun
	@set -eu; \
		stage=$$(mktemp -d); \
		trap 'rm -rf "$$stage"' EXIT HUP INT TERM; \
		sources=$$(RELEASE_SOURCE="$(LATEST_FILE)" $(PYTHON) -c 'from test_sqlstructure import PRODUCTION_FILES; print(" ".join(PRODUCTION_FILES))'); \
		for source in $$sources; do cp "$$source" "$$stage/"; done; \
		cp "$(LATEST_FILE)" "$$stage/serverfiles.py"; \
		cp "$(LATEST_FILE)" "$$stage/dirwalker.py"; \
		chmod +x "$$stage/serverfiles.py" "$$stage/dirwalker.py"; \
		(umask 077; sed 's/^export DB_HOST=.*/export DB_HOST=localhost/' "$(ENV_FILE)" > "$$stage/_serverenv"); \
		rsync -av --exclude='/version[0-9]*.py' --rsync-path=/usr/bin/rsync "$$stage/" "$(REMOTE):$(REMOTEDIR)/"; \
		ssh "$(REMOTE)" 'chmod +x "$$HOME/$(REMOTEDIR)/serverfiles.py"'; \
		cp "$$stage/dirwalker.py" dirwalker.py; \
		chmod +x dirwalker.py
