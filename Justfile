default: start

start:
    uv run --package api uvicorn --reload --app-dir services/api/src api.app:app

sync uv:
    uv lock
    uv sync --all-packages