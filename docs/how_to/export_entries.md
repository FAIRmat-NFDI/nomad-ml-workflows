# Export entries

Use **Export Entries Action** (`nomad_ml_workflows.actions:export_entries`) to save local NOMAD archives as JSON, CSV, or Parquet. The [tutorial](../tutorial/tutorial.md) demonstrates the GUI and a Python REST submission.

## Prerequisites

You need access to the action and source entries, a writable staging project/upload for the output, and a running CPU action worker. Administrators can follow [installation and configuration](install_this_plugin.md).

## Select entries and content

1. Find your entries in a NOMAD search app and copy the query object from **View API Call**.
1. Open **Actions → Run action → Export Entries Action** and set **Destination project ID** to your staging upload ID.
1. Choose the ownership scope, a positive maximum entry count, and the search query. In the GUI, enter the query as JSON text; in an input JSON file, encode it as a string.
1. Leave **Archive required paths** empty for full archives, or add include paths and enable reference resolution where needed.
1. Select `json`, `parquet`, or `csv`, choose ZIP or directory output, and run the action.

| Input | Meaning |
| --- | --- |
| `upload_id` | Destination staging upload; separate from the uploads selected by the query |
| `search_settings.owner` | `visible` (default), `public`, `user`, `shared`, or `staging` |
| `search_settings.max_entries` | Positive limit, bounded by the deployment cap; default is at most 1,000 |
| `search_settings.query` | Search query as JSON text; selects entries |
| `search_settings.required` | Include directives with `type`, `path`, and `resolve_references`; selects archive content |
| `export_settings.file_format` | `parquet` (default), `csv`, or `json` |
| `export_settings.create_zip_archive` | `true` (default) for ZIP; `false` for an upload subdirectory |

For example, this action input selects up to 100 public entries with `entry_type` equal to `ELNSample`, then exports their `results.method` sections as Parquet:

```json
{
  "upload_id": "YOUR_STAGING_UPLOAD_ID",
  "search_settings": {
    "owner": "public",
    "max_entries": 100,
    "query": "{\"entry_type\": \"ELNSample\"}",
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

Replace the query and paths with ones appropriate to your entries. An include path does not require the entry to contain that path; search filters and archive-content selection have [different roles](../explanation/explanation.md#search-selection-and-archive-content). To follow references beneath an included path, set its `resolve_references` to `true`. Exclusion directives are not supported by the public input model.

For REST, wrap this input in `{"data": input_json}` as shown in the tutorial. Omit `user_id`; NOMAD supplies the authenticated user's ID.

## Retrieve and inspect the export

Monitor the action instance's status and logs. After success, its `exported_dir_path` gives the ZIP filename or directory path relative to the destination upload's raw files; `workflow_duration` reports elapsed seconds. Open the project's files to download it.

| Artifact | Contents |
| --- | --- |
| `data.json` | JSON array of records containing `entry_id`, `upload_id`, and the selected `archive` |
| `data.csv` | Flattened columns, with nested values as JSON text |
| `data.parquet/part-NNNNN.parquet` | Parquet parts sharing a schema; nested quantity values are preserved |
| `selected_entries.json` | Selected `entry_id`/`upload_id` pairs |
| `metadata.json` | Inputs, counts, timing, deployment/package versions, and a JSON schema |

Only the selected data format is produced. Read the whole extracted Parquet directory with PyArrow:

```python
import pyarrow.parquet as pq

table = pq.read_table('data.parquet')
```

Compare `num_entries_available`, `num_entries_selected`, and `num_entries_exported` under metadata's `data` object. The manifest describes selection; archive-read or flattening failures may reduce the exported count. Inspect logs for skipped entries. A zero-match export contains metadata and an empty manifest, with no data artifact. See [manifest and count semantics](../explanation/explanation.md#manifest-metadata-and-counts).
