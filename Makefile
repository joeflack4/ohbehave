SRCPATH := $(CURDIR)
PROJECTNAME := $(shell basename $(CURDIR))

define HELP
Manage $(PROJECTNAME). Usage:

make run        - Run $(PROJECTNAME).
make deploy     - Install requirements and run app for the first time.
make update     - Update pip dependencies via Python Poetry.
make clean      - Remove cached files and lock files.
endef
export HELP

.PHONY: run deploy update format clean help


requirements: .requirements.txt
env: .venv/bin/activate


.requirements.txt: requirements.txt
	$(shell . .venv/bin/activate && pip install -r requirements.txt)


all help:
	@echo "$$HELP"


.PHONY: run
run: env
	$(shell . .venv/bin/activate && flask run)


# TODO: bifurcate into production/local
#  - prod?: gunicorn graph:app.server -b :8000
.PHONY: deploy
deploy:
	# $(shell . ./scripts/deploy.sh)
	python3 ohbehave/app.py


.PHONY: update
update: env
	.venv/bin/python3 -m pip install -U pip
	poetry update
	poetry export -f requirements.txt --output requirements.txt --without-hashes


.PHONY: clean
clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	find . -name 'poetry.lock' -delete
	find . -name 'Pipefile.lock' -delete
	rm -rf .pytest_cache
	rm -rf htmlcov

.PHONY: export-csv
export-csv:
	python scripts/export_csv.py
