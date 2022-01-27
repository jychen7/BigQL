install:
	poetry install

test:
	poetry run pytest tests

fmt:
	poetry run black .

fmt-check:
	poetry run black . --check

emulator:
	docker run --rm -d gcr.io/google.com/cloudsdktool/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
