# Export ten entries through the GUI or REST

This tutorial exports up to ten entries visible to you as JSON, packaged in a ZIP saved to a staging project/upload. Both interfaces below run **Export Entries** with the same inputs.

## Before you start

- Sign in to a NOMAD deployment with **Export Entries Action** enabled.
- Create or choose a writable, unpublished project/upload and copy its ID.
- Ask the administrator to confirm that the CPU worker is running. It must poll Temporal's task queue to execute the workflow; REST submission can succeed while execution waits for a worker.

## Use the GUI

1. Open **Actions** and select **Run action**, then choose **Export Entries Action**.
1. Enter the staging upload ID in **Destination project ID**.
1. Under **Search options**, choose **All entries visible to me (visible)**, set **Maximum entries** to `10`, and enter `{}` in **Search query**. Leave **Archive required paths** empty to export complete archives.
1. Under **Export options**, choose `json` and keep **Create ZIP archive** enabled. Run the action.
1. Open the created action instance to follow its status and logs. Once it succeeds, find `exported_dir_path` in **Results**.
1. Open the destination project's files and download the ZIP at that relative path. Extract it to inspect `data.json`, `selected_entries.json`, and `metadata.json`.

!!! note "Screencast placeholder"
    A screencast will demonstrate choosing Export Entries, completing these fields, monitoring the action, and downloading the ZIP. The written steps above are complete without it.

## Submit the same export with Python and REST

You need Python with `requests` installed, network access to the deployment's API, and a bearer token authorized to start actions and access the chosen entries and destination. The client does not need a local NOMAD installation or worker.

Save this as `export_entries_input.json`, replacing `YOUR_STAGING_UPLOAD_ID`:

```json
{
  "upload_id": "YOUR_STAGING_UPLOAD_ID",
  "search_settings": {
    "owner": "visible",
    "max_entries": 10,
    "query": "{}",
    "required": []
  },
  "export_settings": {
    "file_format": "json",
    "create_zip_archive": true
  }
}
```

`query` is a string containing a JSON object. Here `"{}"` matches entries within the chosen ownership scope. Omit `user_id`; REST injects the authenticated user's ID before validating the action inputs and overwrites any supplied value.

Set `NOMAD_API_URL` to the deployment's API base URL, including `/api/v1` (for example, `https://YOUR_NOMAD_HOST/api/v1`), and `NOMAD_TOKEN` to your bearer token in your shell environment. Save the following as `submit_export.py` and run `python submit_export.py` from the directory containing the input file:

```python
import json
import os
from pathlib import Path

import requests

api_url = os.environ['NOMAD_API_URL'].rstrip('/')
token = os.environ['NOMAD_TOKEN']
input_json = json.loads(Path('export_entries_input.json').read_text(encoding='utf-8'))
action_id = 'nomad_ml_workflows.actions:export_entries'

response = requests.post(
    f'{api_url}/actions/{action_id}/start',
    headers={'Authorization': f'Bearer {token}'},
    json={'data': input_json},
    timeout=30,
)
response.raise_for_status()
print(response.json()['action_instance_id'])
```

The script submits the action and prints its instance ID. It does not wait for completion. Use that ID on the GUI action instance page, or make authenticated requests to `GET /actions/{action_instance_id}/status` and, after completion, `GET /actions/{action_instance_id}/result`, relative to the same API base URL. The result's `exported_dir_path` identifies the ZIP in your destination upload's raw files.

## Check the dataset

`data.json` contains a JSON array of records with entry IDs, upload IDs, and archives. `selected_entries.json` records the selected sources. In `metadata.json`, inspect `data.num_entries_available`, `data.num_entries_selected`, and `data.num_entries_exported`. With enough readable entries, the selected count is ten; fewer matches or skipped archives can produce fewer exported records.

If no entries match, the ZIP still contains an empty manifest and metadata, but no `data.json`. For selected archive paths, other formats, and directory output, continue with [Export entries](../how_to/export_entries.md).
