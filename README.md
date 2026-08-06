# nomad-ml-workflows

A NOMAD plugin for supporting machine-learning workflows. It currently provides the
Export Entries action, which turns a permission-scoped NOMAD search into a reusable
JSON, Parquet, or CSV dataset.


## 📦 Installation

Install the package with pip:

```sh
pip install nomad-ml-workflows
```

To add the plugin to your NOMAD deployment, see
[Adding this plugin to NOMAD](#-adding-this-plugin-to-nomad).

## ✨ Key functionality

The Export Entries action lets users:

- select entries with a NOMAD search query and ownership scope;
- export complete archives or selected archive paths, optionally resolving references;
- generate JSON, Parquet, or CSV datasets; and
- save the generated dataset to a staging project/upload as a ZIP archive or directory.

JSON preserves the selected archive structure. Parquet provides typed tabular data
and preserves nested quantity values, while CSV represents nested values as JSON text.

### Action input

The action form groups its fields into **Search options** and **Export options**. Its
underlying input shape is:

```json
{
  "user_id": "<injected by NOMAD>",
  "upload_id": "<destination-project-id>",
  "search_settings": {
    "owner": "visible",
    "max_entries": 1000,
    "query": "{\n  \"entry_type\": \"ELNSample\"\n}",
    "required": [
      {
        "type": "include",
        "path": "results.method",
        "resolve_references": false
      }
    ]
  },
  "export_settings": {
    "file_format": "parquet",
    "create_zip_archive": true
  }
}
```

`user_id` is required by the workflow but is not shown in the action form. The other
fields have these semantics:

| Field | Values and behavior |
| --- | --- |
| `upload_id` | ID of the staging project/upload that receives the exported artifacts. |
| `search_settings.owner` | `visible`, `public`, `user`, `shared`, or `staging`; defaults to `visible`. |
| `search_settings.max_entries` | Positive requested limit, bounded by `max_entries_export_limit`; defaults to the smaller of 1,000 and the deployment limit. |
| `search_settings.query` | NOMAD search query supplied as JSON text. The query can be copied from **View API Call** in a NOMAD search app. |
| `search_settings.required` | Archive paths to include. An empty list exports the complete archive. Set `resolve_references` to include content reached through references below a path. |
| `export_settings.file_format` | `parquet`, `csv`, or `json`; defaults to `parquet`. |
| `export_settings.create_zip_archive` | Defaults to `true`. Set it to `false` to publish a project subdirectory instead of a ZIP file. |

The current public model supports include directives only. Exclusion is not yet
available through the action form.

### Generated output

The generated ZIP archive or directory contains:

| File | Contents |
| --- | --- |
| `data.json`, `data.parquet`, or `data.csv` | The exported archive data in the selected format. This file is omitted when no entries match. |
| `selected_entries.json` | The ordered `entry_id` and `upload_id` pairs selected for export. |
| `metadata.json` | The export metadata under `data`, together with its JSON schema and an explanatory note. |

The metadata records the original input, search timing, export-limit state, and the
`num_entries_available`, `num_entries_selected`, and `num_entries_exported` counts. It
also records `nomad_deployment_api_host`, `nomad_version`, and
`nomad_ml_workflows_version`. A zero-match export still contains `metadata.json` and
an empty `selected_entries.json`.

## ⚙️ Configuration

Configure the action entry point in the `nomad.yaml` of the NOMAD Oasis:

```yaml
plugins:
  entry_points:
    options:
      nomad_ml_workflows.actions:export_entries:
        max_entries_export_limit: 100000
        # Deployment cap for one Export Entries action.

        read_archives_timeout: 7200
        # Start-to-close timeout, in seconds, for reading archives and
        # writing the selected output format.

        max_write_buffer_size_bytes: 4194304
        # Target maximum uncompressed Arrow bytes accumulated before a
        # Parquet or CSV flush. One oversized row may exceed this target.
```

## 🚀 Adding this plugin to NOMAD

### NOMAD Oasis

Follow the
[NOMAD plugin installation documentation](https://nomad-lab.eu/prod/v1/staging/docs/howto/oasis/plugins_install.html)
to add and enable the plugin in an Oasis.

### Local NOMAD development installation

Use the dedicated
[`nomad-distro-dev`](https://github.com/FAIRmat-NFDI/nomad-distro-dev) repository for
an integrated local NOMAD development environment.

## 🛠️ Development

Clone the repository and create a virtual environment with Python 3.10, 3.11, or
3.12:

```sh
git clone https://github.com/FAIRmat-NFDI/nomad-ml-workflows.git
cd nomad-ml-workflows
python3.12 -m venv .pyenv
. .pyenv/bin/activate
python -m pip install --upgrade pip
python -m pip install uv
uv pip install -e '.[dev]'
```

Run the focused Export Entries tests:

```sh
pytest tests/actions/export_entries/test_models.py \
  tests/actions/export_entries/test_utils.py
```

Run linting and formatting checks with Ruff:

```sh
ruff check .
ruff format . --check
```

For interactive test debugging, pass `--pdb` to pytest. To serve the documentation
locally, use the development dependencies and run:

```sh
mkdocs serve
```

## 👥 Main contributors

| Name | Email |
| --- | --- |
| Sarthak Kapoor | [sarthak.kapoor@physik.hu-berlin.de](mailto:sarthak.kapoor@physik.hu-berlin.de) |

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
