install:
	poetry install

test:
	poetry run pytest tests

build:
	poetry build

publish:
	poetry publish

fmt:
	poetry run black .

fmt-check:
	poetry run black . --check

emulator:
	docker run --rm -d gcr.io/google.com/cloudsdktool/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086

e2e_test:
	poetry run pytest e2e_tests
