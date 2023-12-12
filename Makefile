FILES := $(shell git diff --name-only --diff-filter=AM $$(git merge-base origin/main HEAD) -- \*.py)


.PHONY: test
test:
	pytest

.PHONY: lint
lint:
	flake8 --filename ./$(FILES)

.PHONY: format
format:
ifneq ("$(FILES)","")
	black $(FILES)
	isort $(FILES)
endif

.PHONY: tidy
tidy: format lint

# Removes the directory that contains bytecode cache files
# that are automatically generated by python.
.PHONY: clean
clean:
	find . -type f -name "*.pyc" | xargs rm -fr
	find . -type d -name __pycache__ | xargs rm -fr