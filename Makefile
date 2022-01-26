init:
	poetry init

test:
	poetry run pytest tests

fmt:
	poetry run black .
