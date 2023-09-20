default: build

lint-black:
	poetry run black samples/ tests/ gambol/

lint-flake8:
	poetry run flake8 samples/ tests/ gambol/

lint-mypy:
	poetry run mypy .

lint: lint-black lint-flake8 lint-mypy

build:
	poetry self add "poetry-dynamic-versioning[plugin]"
	poetry build

publish: build
	poetry publish -r publish

test:
	poetry run pytest -vv -s -o log_cli=true -o log_cli_level=DEBUG -o cache_dir=/tmp tests/$(test)

coverage:
	poetry run coverage run --source='./gambol/' -m pytest -v --junitxml junit-report.xml tests/ && coverage xml && coverage report -m