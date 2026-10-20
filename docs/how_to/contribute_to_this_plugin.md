# Contribute to This Plugin

## Set up development

We recommend using [uv](https://docs.astral.sh/uv/) to manage Python environments and dependencies, with Python 3.10 or newer. Clone the repository and install the development dependencies:

```sh
git clone https://github.com/FAIRmat-NFDI/nomad-ml-workflows.git
cd nomad-ml-workflows
uv sync --python 3.12 --extra dev
. .venv/bin/activate
```

Run the commands below from the plugin repository root.

## Run tests

Run the full test suite:

```sh
uv run pytest tests/
```

To run a subset, pass a test directory or file:

```sh
uv run pytest tests/actions/export_entries/
uv run pytest tests/actions/export_entries/test_models.py
```

Use `-k EXPRESSION` to select tests by name and `-v` for verbose output.

## Check code and documentation

Run linting and formatting checks with Ruff:

```sh
uv run ruff check .
uv run ruff format . --check
```

To apply automatic formatting, run `uv run ruff format .`.

Markdown linting requires Node.js 22 or newer and npm/npx:

```sh
uv run poe markdown-lint
```

Add `--fix` to apply automatic Markdown fixes: `uv run poe markdown-lint --fix`.

## Preview documentation

Serve the documentation locally:

```sh
uv run mkdocs serve
```

Before submitting documentation changes, check that the site builds without warnings:

```sh
uv run mkdocs build --strict
```
