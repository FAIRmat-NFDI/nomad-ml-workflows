# Export functionality

The three export actions share a sequence: search entries using the initiating user's permissions, record the selected entry IDs in a manifest, read their archives, write the requested format, and publish the artifacts. Export Entries supplies this foundation; Export Forces converts archives into atomic structures, while Export Remote Entries runs extraction on selected deployments and saves each dataset to S3. You can start any action independently.

## Search selection and archive content

`search_settings.query` is a NOMAD search query encoded as **JSON text**, for example `'{"entry_type": "ELNSample"}'` in a JSON input file. You can write the query yourself. Or you can use a search app to make a selection and copy the query object from **View API Call**; this is the recommened way. Make sure to only copy the query object rather than the whole API request body. The [tutorial](../tutorial/tutorial.md#use-the-gui) shows where to use it. The query determines which entries match.

`search_settings.owner` determines the ownership scope:

| Value | Eligible entries |
| --- | --- |
| `visible` | All entries visible to the initiating user; the default |
| `public` | Public entries |
| `user` | The user's entries |
| `shared` | The user's entries and entries shared with them |
| `staging` | The user's and shared unpublished entries |

`max_entries` is a positive selection limit. Its default is the smaller of 1,000 and the configured deployment cap. The action selects up to the applicable limit from the search results; it does not sample randomly.

For Export Entries and Export Remote Entries, `search_settings.required` controls **what is read from each selected archive**. An empty list requests the complete archive. Both include and exclude directives use dot-separated section or quantity paths, such as `results.method`.

- `type: include` selects a path. Set `resolve_references: true` to include data reached through references below it, subject to access permissions.
- `type: exclude` removes a path. An exclude-only list starts from the complete archive and removes the specified paths.

Choosing a path does not filter out entries where that path is absent; use the search query to require matching content. See the [action form inputs](../how_to/export_entries.md#action-form-inputs) for examples.

Export Forces instead uses `search_settings.properties` to select energies, forces, and stresses. It adds corresponding quantity-presence conditions to its search query and reads the archive data needed by the forces exporter.

## Formats and packaging

| Format | Representation |
| --- | --- |
| `json` | A JSON array of records containing `entry_id`, `upload_id`, and the selected `archive`; preserves archive structure |
| `parquet` | One row per exported entry, with archive sections flattened into columns and nested quantity values preserved; written as a directory of part files (shards) sharing one schema |
| `csv` | Flattened archive columns; nested values are encoded as JSON text in cells |
| `extxyz` / `ase_db` | Atomic structures and selected properties, available through Export Forces |

Tabular export follows the installed NOMAD metainfo definitions. Data without a corresponding current schema definition may be omitted or an entry may be skipped; consult the action logs when counts differ.

`export_settings.create_zip_archive` defaults to `true`. Export Entries and Export Forces put a ZIP into the destination upload's raw files on the deployment where you start the action. With `false`, they create a subdirectory containing the same artifacts. Remote export uses an S3 object for ZIP output or an S3 prefix for directory output. In that action, **local** means the initiating deployment; optional copying saves the remote datasets into an upload on that deployment.

## Manifest, metadata, and counts

Every successful export includes `selected_entries.json`, an ordered list of selected `entry_id`/`upload_id` pairs, and `metadata.json`. Metadata contains a `note`, a `schema`, and a `data` object with the original inputs, search timestamps, deployment host, package versions, and these fields:

| Field under `data` | Meaning |
| --- | --- |
| `num_entries_available` | Total search matches before applying the selection limit |
| `num_entries_selected` | Entries recorded in the manifest after applying the limit |
| `num_entries_exported` | Exported archive records or tabular rows for Export Entries; Export Forces records the selected manifest count, which is not a count of atomic configurations |
| `reached_max_entries_limit` | Whether the total matches exceeded the selection limit |
| `error_info` | Extraction error information, when recorded |

The manifest records selection, not a guarantee that every archive could be read or converted. Check counts and logs before using a dataset. A search with zero matches still produces metadata and an empty manifest, but no data artifact. This applies to the forces export and each remote extraction as well.

Remote export returns [per-target results](../how_to/export_remote_entries.md#inspect-the-results), where counts are summed across targets.

## Models and datasets

Exports produce files that you can use in downstream training. The separate [MLModel and Dataset schemas](schema.md) describe model artifacts and their provenance. Running an export does not automatically create an MLModel or Dataset entry, train a model, or compute evaluation metrics.
