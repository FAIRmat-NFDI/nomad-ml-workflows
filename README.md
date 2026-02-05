# nomad-ml-workflows

A NOMAD plugin for managing ML workflows. Currently, it provides an action to export large number of entries from NOMAD database as tabular data files. Other ML workflow related actions and schemas will be added in future.

## 📦 Installation
You can install the plugin using pip:
```sh
pip install nomad-ml-workflows @ git+https://github.com/FAIRmat-NFDI/nomad-ml-workflows.git
```

However, to fully utilize the plugin, you need to add it to your NOMAD instance as described [below](#-adding-this-plugin-to-nomad).

## ✨ Features

- Export a large number of NOMAD entries as tabular data files (CSV, Parquet) using NOMAD Actions. Once the action is triggered, it will:
  - Search entries based on user-defined criteria.
  - Optionally include or exclude data fields from the entries.
  - Package the entries into tabular data files like CSV or Parquet (or as JSON)
  - Export the files to a specified Project (or previously known as Upload) in NOAMD.

  These can then be downloaded from the NOMAD web interface for local use.

## ⚙️ Configuration
The Export Entries action can be configured using the following parameters in
the `nomad.yaml` configuration file of your NOMAD Oasis instance:

```yaml
plugins:
  entry_points:
    options:
      nomad_ml_workflows.actions:export_entries:
        search_batch_timeout: 7200
        # Timeout (in seconds) for each search batch in the Export Entries
        # action. Set this accordingly to time out longer searches.
        max_entries_export_limit: 100000
        # Maximum number of entries that can be exported in a single
        # Export Entries action.
```


## 🚀 Adding this plugin to NOMAD

Currently, NOMAD has two distinct flavors that are relevant depending on your role as an user:
1. [A NOMAD Oasis](#adding-this-plugin-in-your-nomad-oasis): any user with a NOMAD Oasis instance.
2. [Local NOMAD installation and the source code of NOMAD](#adding-this-plugin-in-your-local-nomad-installation-and-the-source-code-of-nomad): internal developers.

### Adding this plugin in your NOMAD Oasis

Read the [NOMAD plugin documentation](https://nomad-lab.eu/prod/v1/staging/docs/howto/oasis/plugins_install.html) for all details on how to deploy the plugin on your NOMAD instance.

### Adding this plugin in your local NOMAD installation and the source code of NOMAD

We now recommend using the dedicated [`nomad-distro-dev`](https://github.com/FAIRmat-NFDI/nomad-distro-dev) repository to simplify the process. Please refer to that repository for detailed instructions.


## 🛠️ Development

If you want to develop locally this plugin, clone the project and in the plugin folder, create a virtual environment (you can use Python 3.10, 3.11 or 3.12):
```sh
git clone https://github.com/FAIRmat-NFDI/nomad-ml-workflows.git
cd nomad-ml-workflows
python3.11 -m venv .pyenv
. .pyenv/bin/activate
```

Make sure to have `pip` upgraded:
```sh
pip install --upgrade pip
```

We recommend installing `uv` for fast pip installation of the packages:
```sh
pip install uv
```

Install the `nomad-lab` package:
```sh
uv pip install -e '.[dev]'
```

### Run linting and auto-formatting

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting the code. Ruff auto-formatting is also a part of the GitHub workflow actions. You can run locally:
```sh
ruff check .
ruff format . --check
```

### Debugging

For interactive debugging of the tests, use `pytest` with the `--pdb` flag. We recommend using an IDE for debugging, e.g., _VSCode_. If that is the case, add the following snippet to your `.vscode/launch.json`:
```json
{
  "configurations": [
      {
        "name": "<descriptive tag>",
        "type": "debugpy",
        "request": "launch",
        "cwd": "${workspaceFolder}",
        "program": "${workspaceFolder}/.pyenv/bin/pytest",
        "justMyCode": true,
        "env": {
            "_PYTEST_RAISE": "1"
        },
        "args": [
            "-sv",
            "--pdb",
            "<path-to-plugin-tests>",
        ]
    }
  ]
}
```

where `<path-to-plugin-tests>` must be changed to the local path to the test module to be debugged.

The settings configuration file `.vscode/settings.json` automatically applies the linting and formatting upon saving the modified file.

### Documentation on Github pages

To view the documentation locally, install the related packages using:
```sh
uv pip install -r requirements_docs.txt
```

Run the documentation server:
```sh
mkdocs serve
```


## 👥 Main contributors
| Name | E-mail     |
|------|------------|
| Sarthak Kapoor | [sarthak.kapoor@physik.hu-berlin.de](mailto:sarthak.kapoor@physik.hu-berlin.de)


## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
