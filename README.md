# nomad-ml-workflows

A NOMAD plugin for exporting datasets and describing machine learning models.

- **Export Entries:** Export complete archives or selected paths as JSON, CSV, or Parquet.
- **Export Forces:** Export atomic structures with energies, forces, and stresses as Extended XYZ or an ASE database.
- **Export Remote Entries:** Export datasets from configured Oases to S3, optionally copying them into a local upload.
- **MLModel and Dataset schemas:** Describe model artifacts, training data, and evaluation metadata.

## 📦 Installation

Install the plugin with its export-worker dependencies:

```sh
pip install 'nomad-ml-workflows[cpu-action]'
```

Export actions require a configured NOMAD deployment and a running CPU action worker.

## 📚 Documentation

See the [documentation](https://fairmat-nfdi.github.io/nomad-ml-workflows/) for the GUI and REST tutorial, action guides, deployment configuration, and schema examples.

## 🚀 Adding this plugin to NOMAD

### NOMAD Oasis

Follow the
[NOMAD Oasis configuration documentation](https://docs.nomad-lab.eu/howto/oasis/configure.html#plugins)
to add and enable the plugin in an Oasis.

### Local NOMAD development installation

Use the dedicated
[`nomad-distro-dev`](https://github.com/FAIRmat-NFDI/nomad-distro-dev) repository for
an integrated local NOMAD development environment.

## 🛠️ Development

We recommend using [uv](https://docs.astral.sh/uv/) to manage Python environments
and dependencies, with Python 3.10 or newer.

Clone the repository and install the development dependencies:

```sh
git clone https://github.com/FAIRmat-NFDI/nomad-ml-workflows.git
cd nomad-ml-workflows
uv sync --python 3.12 --extra dev
. .venv/bin/activate
```

Run the full test suite from the repository root after installing the development dependencies:

```sh
uv run pytest tests/
```

To run a subset, pass a test directory or file:

```sh
uv run pytest tests/actions/export_entries/
uv run pytest tests/actions/export_entries/test_models.py
```

Use `-k EXPRESSION` to select tests by name and `-v` for verbose output.

Run linting and formatting checks with Ruff:

```sh
uv run ruff check .
uv run ruff format . --check

# run auto-formatting
uv run ruff format .
```

Run markdown linting when working with the docs:

```sh
# requires Node.js 22+ and npm/npx

uv run poe markdown-lint --fix
```

To serve the documentation locally, use the development dependencies and run:

```sh
uv run mkdocs serve
```

## License

Licensed under the MIT License. See [LICENSE](LICENSE).
