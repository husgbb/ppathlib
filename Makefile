.PHONY: clean clean-pyc clean-test clean-build dist format install lint release release-test reqs test test-debug help
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
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

dist: clean ## build source and wheel package
	python3 -m build
	ls -l dist

format:  ## run black to format codebase
	black ppathlib tests

help:
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

install: clean ## install the package to the active Python's site-packages
	python3 -m pip install -e .[all]

lint: ## check style with black, flake8, and mypy
	black --check ppathlib tests
	flake8 ppathlib tests
	mypy ppathlib

release: dist ## package and upload a release
	twine upload dist/*

release-test: dist
	twine upload --repository pypitest dist/*

reqs:  ## install development requirements
	python3 -m pip install -U -r requirements-dev.txt

test: ## run tests
	python3 -m pytest -vv

test-debug:  ## rerun tests that failed in last run and stop with pdb at failures
	python3 -m pytest -n=0 -vv --lf --pdb
