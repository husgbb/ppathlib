.PHONY: clean clean-pyc clean-test clean-build dist format install lint reqs test test-debug help
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

clean: clean-build clean-pyc clean-test ## remove build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -path ./.venv -prune -o -name '*.egg-info' -exec rm -fr {} +
	find . -path ./.venv -prune -o -name '*.egg' -exec rm -rf {} +

clean-pyc: ## remove Python file artifacts
	find . -path ./.venv -prune -o -name '*.pyc' -exec rm -f {} +
	find . -path ./.venv -prune -o -name '*.pyo' -exec rm -f {} +
	find . -path ./.venv -prune -o -name '*~' -exec rm -f {} +
	find . -path ./.venv -prune -o -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

dist: clean ## build source and wheel package
	uv run python -m build
	ls -l dist

format:  ## run formatters
	uv run ruff format ppathlib tests

help:
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

install: clean ## install the package to the active Python's site-packages
	uv sync

lint: ## check style with ruff and ty
	uv run ruff check ppathlib tests
	uv run ty check ppathlib tests

reqs:  ## install development requirements
	uv sync

test: ## run tests
	uv run pytest -vv

test-debug:  ## rerun tests that failed in last run and stop with pdb at failures
	uv run pytest -vv --lf --pdb
