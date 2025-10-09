# Strenger Bash + .env laden
set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := true

# Sauberer PYTHONPATH für beide Pakete
export PYTHONPATH := "packages/gaebio/src:packages/gaebdb/src:{{env_var('PYTHONPATH','')}}"

# ----- Default -----
default: api

# ----- API dev server -----
api:
	uv run --package api uvicorn --reload --app-dir services/api/src api.app:app

# ----- uv sync -----
sync-uv:
	uv lock
	uv sync --all-packages

# ----- gaebio quick test -----
gaebio-test:
	PYTHONPATH=packages/gaebio/src \
	uv run python packages/gaebio/src/gaebio/try_parsing.py packages/gaebio/tests/data/sample.X83

# ----- DB connect -----
db-connect:
	psql "postgresql://neondb_owner:npg_tMUI4YA0GFOa@ep-lingering-thunder-a9zfoyxc-pooler.gwc.azure.neon.tech/neondb?sslmode=require&channel_binding=require"

# ----- DB init (nur Tabellen anlegen) -----
db-init:
	uv run python -m gaebdb.main

# ----- DB save (X83/X84 importieren; Args werden durchgereicht) -----
# Beispiel:
#   just db-save --gaeb packages/gaebio/tests/data/sample.X83 --phase X83 --project "Demo"
db-save *ARGS:
	uv run python -m gaebdb.main {{ARGS}}

# ----- Preise aus X84 anwenden -----
# Beispiel:
#   just db-price --apply-prices-from packages/gaebio/tests/data/sample.X84 --update-key oz_path
db-price *ARGS:
	uv run python -m gaebdb.main {{ARGS}}
