setup:
	-uv init
	uv sync

test:
	uv run pytest --verbose