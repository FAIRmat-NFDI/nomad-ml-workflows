# NOMAD ML Workflows

`nomad-ml-workflows` helps you turn NOMAD entries into reusable datasets and describe machine learning models alongside their artifacts, training metadata, and evaluations.

## Choose an export action

| Action | Use it for | Output |
| --- | --- | --- |
| [Export Entries](how_to/export_entries.md) | Complete archives or selected archive paths from this deployment | JSON, CSV, or a Parquet dataset in a staging project/upload |
| [Export Forces](how_to/export_forces.md) | Atomic structures with selected energies, forces, and stresses; shown as **Export MLIP Data Action** in the GUI | Extended XYZ or an ASE database in a staging project/upload |
| [Export Remote Entries](how_to/export_remote_entries.md) | Entries from configured Oases, optionally including this deployment | One export per target in S3, optionally copied into a staging project/upload on the deployment that starts the action |

Each action runs independently. They share permission-scoped search and archive extraction, with Export Entries providing the foundation. Read the [export explanation](explanation/export_entries.md) for selection, formats, manifests, and counts.

## Start here

Follow the short [Export Entries tutorial](tutorial/tutorial.md) to save up to ten visible entries as JSON in a ZIP, using either the GUI or a Python REST client. Use the [action chooser](how_to/use_this_plugin.md) for task-specific guides.

Administrators can follow [installation and configuration](how_to/install_this_plugin.md) to enable the plugin, set limits and action access, and configure remote targets and storage.

## Describe models and datasets

The [Schema explanation](explanation/schema.md) introduces the ML Model and Dataset schemas. Describe model versions, libraries, files, training datasets, and evaluation metrics in an MLModel entry. Use Dataset to describe data through NOMAD references, files, or URLs.
