# Choose an export action

Sign in to a deployment with the plugin enabled and an active CPU action worker. For output saved to NOMAD, create or choose a writable staging project/upload and copy its ID. Source entries may belong to other uploads, provided you can read them.

| Your task | Guide | GUI action name |
| --- | --- | --- |
| Export local archives or selected paths as JSON, CSV, or Parquet | [Export entries](export_entries.md) | Export Entries Action |
| Export atomic structures with energies, forces, or stresses | [Export forces](export_forces.md) | Export MLIP Data Action |
| Search configured Oases and save results in S3 | [Export remote entries](export_remote_entries.md) | Export Remote Entries Action |

For a first export, follow the [GUI and REST tutorial](../tutorial/tutorial.md). All JSON action-input examples in these guides omit `user_id`: NOMAD injects the authenticated user's ID when starting an action through REST, including from the GUI.

Open **Actions**, select **Run action**, choose an action, complete its form, and submit it. Use the action instance page to monitor status, inspect results, and read logs. Actions available to you depend on the deployment's configured user/group access.

If an action is missing or remains waiting to execute, consult the administrator's [installation guide](install_this_plugin.md). For format and count semantics, read the [export explanation](../explanation/explanation.md).
