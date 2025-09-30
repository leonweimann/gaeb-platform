default: api

api:
    uv run --package api uvicorn --reload --app-dir services/api/src api.app:app

sync uv:
    uv lock
    uv sync --all-packages

gaebio test:
    PYTHONPATH=packages/gaebio/src \
    uv run python packages/gaebio/src/gaebio/try_parsing.py packages/gaebio/tests/data/sample.X83
