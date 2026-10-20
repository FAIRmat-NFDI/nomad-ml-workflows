# Export energies, forces, and stresses

Use **Export MLIP Data Action** (`nomad_ml_workflows.actions:export_forces`) to export atomic structures and selected properties for machine learning interatomic potentials. Its formats are Extended XYZ (`extxyz`) and ASE database (`ase_db`).

Currently only `DFT` entries are supported.

`magmoms` are currently only added for `VASP DFT` entries.

## Prerequisites

You need access to the action and compatible calculation archives, a writable staging project/upload, and a running CPU action worker with the `cpu-action` dependencies installed, including `nomad-forces-export`. See [administrator setup](install_this_plugin.md).

## Configure and run

1. Open **Actions → Run action → Export MLIP Data Action** and set the destination staging upload ID.
1. Choose an ownership scope and maximum entry count. Keep or adapt the prefilled calculation query for your source data.

```Json
{ 
  "results.method.method_name:any": [ "DFT" ],   
  "quantities:all": 
    [ "run.calculation", 
      "run.system" ] 
}
```

1. Under **Properties**, select energies, forces, and/or stresses. The underlying field names are exactly `energies`, `forces`, and `stresses`; each defaults to `false`.
1. Choose `extxyz` and/or `ase_db`, select ZIP or directory output, and run the action.
1. Inspect status, logs, and results, then download the output from the destination project's files using `exported_dir_path`.

The action adds quantity-presence conditions for the selected properties to the query (`run.calculation.energy.total_t0.value`, `run.calculation.forces.total.value`, and `run.calculation.stress.total.value`). Use archives supported by `nomad-forces-export`; general ELN entries need not contain compatible structures or calculation data.

## Example input

This input requests energy and force data from up to 100 public matching entries. `{}` adds no user filters; the action still adds the selected property requirements. For a narrower dataset, use a query copied from a search app's **View API Call** dialog.

```json
{
  "upload_id": "YOUR_STAGING_UPLOAD_ID",
  "search_settings": {
    "owner": "public",
    "max_entries": 100,
    "query": "{}",
    "properties": {
      "energies": true,
      "forces": true,
      "stresses": false
    }
  },
  "export_settings": {
    "file_format": "extxyz",
    "create_zip_archive": true
  }
}
```

For REST, omit `user_id` and send the input under `data` to this action's start endpoint. `query` is JSON text. Ownership scopes and limits follow the [shared selection rules](../explanation/export_entries.md#search-selection-and-archive-content). Forces uses `properties` instead of an archive `required` list; its extractor chooses the archive content needed for atomic data.

## Expected output

The ZIP or directory contains `data.xyz` for Extended XYZ or `data.db` for an ASE database, plus `selected_entries.json` and `metadata.json`. Set `file_format` to `ase_db` for database output, or `create_zip_archive` to `false` for a directory. The result also reports `workflow_duration`.

Metadata records source selection, inputs, versions, and timing.

`num_entries_exported` is set to the number of entries that were processed; it does **not** measure the number of exported atomic configurations. An entry can contain multiple configurations, and conversion may skip unsupported frames. The archive filtering checks for relevant properties being present before adding a entry to the count, however due to errors within the archive data there might be no usable frames in that entry.

`num_frames_exported` gives the actual total number of frames exported acroos all the entries.

Inspect logs and the generated atomic dataset to assess its contents.

A zero-match run produces an empty manifest and metadata without `data.xyz` or `data.db`. See the [shared export explanation](../explanation/export_entries.md) for packaging and provenance.
