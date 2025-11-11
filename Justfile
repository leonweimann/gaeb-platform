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
	uv run python packages/gaebio/src/gaebio/try_parsing.py packages/gaebio/tests/data/sample.X84 X84
