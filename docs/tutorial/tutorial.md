# Export ten perovskite solar-cell entries on NOMAD Central

This tutorial exports up to ten entries of type `PerovskiteSolarCell` visible to you on [NOMAD Central](https://nomad-lab.eu/). The complete archives are saved as JSON in a ZIP in your destination project. Both the GUI and Python/REST instructions below run **Export Entries Action** with the same inputs.

## Before you start

1. Open [NOMAD Central's GUI selector](https://nomad-lab.eu/select-gui), select **Launch New GUI**, and sign in with your NOMAD account. **Export Entries Action** is available on this installation.
1. Open [Projects](https://nomad-lab.eu/projects), select **New Project**, enter a **Project name** such as `Perovskite export`, and select **Create**. Leave the files empty.
1. Open the new project's overview and keep the project unpublished so the action can write the export to it.

## Use the GUI

Watch the screencast or follow the steps below:

<video class="theme-video" controls playsinline preload="none"
    aria-label="Export Entries Action GUI walkthrough"
    src="../assets/export_entries_action_light.webm"
    data-theme-light="../assets/export_entries_action_light.webm"
    data-theme-dark="../assets/export_entries_action_dark.webm">
    Your browser does not support embedded video.
    <a href="../assets/export_entries_action_light.webm">Download the screencast</a>.
</video>

1. In the newly created project's overview, select **Run action**, and choose **Export Entries Action**.
1. Confirm that **Destination project ID** is already populated with this project's ID.
1. Under **Search options**, choose **All entries visible to me (visible)**, set **Maximum entries** to `10`, and enter `{"entry_type": "PerovskiteSolarCell"}` in **Search query**. Leave **Archive required paths** empty to export complete archives.
1. Under **Export options**, choose `json` and keep **Create ZIP archive** enabled. Run the action.

The action searches for entries and collects their archives in the background. To follow its progress:

1. Select **Actions** in the left menu to view the actions you have run, along with their project IDs and creation times. Select the most recent action's ID in the first row to open its preview.
1. Select **Open full page** in the preview to follow the action's status and logs. Once it succeeds, find the relative path of the exported artifacts under `exported_dir_path` in **Results**.

Open the destination project's files and find the ZIP at that relative path (for example, `export_entries_<timestamp>.zip`). If the file is not visible, reload the page. Download and extract the ZIP to inspect `data.json`, `selected_entries.json`, and `metadata.json`.

!!! tip "Use a query for your data"
    The query above selects perovskite solar-cell entries. To choose different data, find your entries in a NOMAD search app, open **View API Call**, and copy only the query object. Paste that object as JSON text into **Search query**. When editing an input JSON file, encode the object as a string, as shown below and in the [action form inputs](../how_to/export_entries.md#action-form-inputs).

## Submit the same export with Python and REST

You need Python with `requests` installed and network access to Central's API. The client does not need a local NOMAD installation or worker.

Using the same NOMAD account, open **User Settings → Tokens** ([Central's Tokens page](https://nomad-lab.eu/settings/tokens)). Create a personal access token with a name, an expiration date, and the `actions:run` and `actions:read` scopes, then select **Create token**. Copy the displayed token for `NOMAD_TOKEN` below. Your account must have access to the source entries and write access to the destination project. For more details, see [Authenticate programmatically](https://docs.nomad-lab.eu/howto/manage/program/auth.html).

Copy the ID of the project created above. Save this as `export_entries_input.json`, replacing `YOUR_STAGING_UPLOAD_ID` with that ID; REST requests require you to supply `upload_id`:

```json
{
  "upload_id": "YOUR_STAGING_UPLOAD_ID",
  "search_settings": {
    "owner": "visible",
    "max_entries": 10,
    "query": "{\"entry_type\": \"PerovskiteSolarCell\"}",
    "required": []
  },
  "export_settings": {
    "file_format": "json",
    "create_zip_archive": true
  }
}
```

`query` is a string containing a JSON object. Here it selects entries whose `entry_type` is `PerovskiteSolarCell` within the `visible` ownership scope, matching the GUI example. Omit `user_id`; REST injects the authenticated user's ID before validating the action inputs and overwrites any supplied value.

Set Central's API base URL and your token in your shell environment, replacing the token placeholder with the value copied above:

```sh
export NOMAD_API_URL='https://nomad-lab.eu/backend/api/v1'
export NOMAD_TOKEN='YOUR_PERSONAL_ACCESS_TOKEN'
```

Save the following as `submit_export.py` and run `python submit_export.py` from the directory containing the input file:

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
action_instance_id = response.json()['action_instance_id']
print(action_instance_id)
print(f'https://nomad-lab.eu/actions/{action_instance_id}')
```

The script submits the action and prints its instance ID and Central action-page URL. It does not wait for completion. Open the printed URL to follow the status and logs, then download the ZIP from the destination project's files as described above. You can also make authenticated requests to `GET /actions/{action_instance_id}/status` and, after completion, `GET /actions/{action_instance_id}/result`, relative to the same API base URL. The result's `exported_dir_path` identifies the ZIP in your destination upload's raw files.

## Check the dataset

After completing either interface's instructions, check the export:

1. Confirm that the action succeeded and that the ZIP is available at `exported_dir_path` in the destination project's files.
1. Extract the ZIP and check for `data.json`, `selected_entries.json`, and `metadata.json`.
1. Inspect `data.json`, a JSON array of records with entry IDs, upload IDs, and archives for the selected `PerovskiteSolarCell` entries. Compare the entry and upload IDs with the selected sources in `selected_entries.json`.
1. In `metadata.json`, inspect `data.num_entries_available`, `data.num_entries_selected`, and `data.num_entries_exported`. With enough readable entries, ten are selected and exported. Fewer matches or skipped archives can produce fewer exported records; check the action logs for skipped entries. Matching totals and selected entries can change as Central's data changes.

If no entries match, the ZIP still contains an empty manifest and metadata, but no `data.json`. For selected archive paths, other formats, and directory output, continue with [Export entries](../how_to/export_entries.md).
