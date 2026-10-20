# Export remote entries

Use **Export Remote Entries Action** (`nomad_ml_workflows.actions:export_remote_entries`) to run the same archive extraction on configured Oases over Temporal Nexus.

In this guide, **local** means the current NOMAD deployment from which you start the action, also called the **initiating deployment**. The `local` target extracts entries from that deployment; other targets extract entries from remote deployments. Each target writes its own export to S3. You can also copy successful exports into a staging upload on the initiating deployment.

## Prerequisites

An administrator must configure Nexus endpoints, action access, storage, and CPU workers on participating deployments; see [remote export setup](install_this_plugin.md#configure-remote-export). The initiating user's ID is forwarded to remote extraction, which applies that user's permissions there. Participating deployments must recognize that identity, trust the Nexus callers, and configure matching [payload encryption keys](install_this_plugin.md#configure-nexus-trust-and-payload-encryption).

S3 storage must be accessible to the workers that write exports. Copying datasets back also requires storage access for the initiating deployment's worker and a writable staging upload there. For ZIP copy-back, configure the same bucket name on the initiating and receiving deployments; the current copy implementation uses the initiating bucket when parsing presigned URLs. The staging upload is needed only when `save_to_upload` is `true`.

Action submission through the GUI and HTTP API defaults to `users: [admin]`; administrators must configure the intended user IDs or groups. Incoming Nexus calls do not recheck the receiver's action `users` or `groups` lists, so access to the receiving endpoint must be restricted to trusted callers.

## Choose targets and run

1. Open **Actions → Run action → Export Remote Entries Action**.
1. Choose one or more **Target Oases**. `local` runs extraction on the initiating deployment; other values are administrator-configured Nexus target keys.
1. Set search options, including ownership scope, maximum entries, query JSON text, and optional archive include/exclude directives. The selection limit applies separately to each target.
1. Select `json`, `csv`, or `parquet` and ZIP or directory output.
1. Leave **Save dataset to upload** disabled for S3-only output, or enable it and provide the destination staging upload ID.
1. Run the action, then inspect every target's result.

## Example input

With the administrator's `oasis-b` target configured and deployment-managed storage (`s3_mode: env`), this input exports up to ten visible entries from each of the initiating deployment (`local`) and the remote `oasis-b` deployment:

```json
{
  "target_oases": ["local", "oasis-b"],
  "save_to_upload": false,
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

Use the keys supplied by your administrator. To search only remote data, omit `local` from `target_oases`. To copy each successful dataset to an upload on the initiating deployment, set `save_to_upload` to `true` and add `upload_id`. Each target remains a separate export; the action does not merge datasets.

For REST, omit `user_id` and submit this input under `data`. Search query text and archive include/exclude directives follow [Export Entries](export_entries.md). Remote export does not produce forces formats.

## Storage options

In the default `env` mode, the worker resolves storage from deployment configuration and environment variables, and the form hides `storage_settings`. Supplied storage settings are replaced by deployment-managed values on a deployment running in this mode.

If the administrator enables `s3_mode: workflow_input`, add this `storage_settings` object to the action input:

```json
{
  "storage_settings": {
    "storage_type": "s3",
    "bucket": "ml-datasets",
    "prefix": "exports",
    "region": "eu-central-1"
  }
}
```

This fragment supplements the complete input above. `endpoint_url` can select an S3-compatible service; optional `access_key_id`, `secret_access_key`, and `session_token` supply explicit credentials. Without explicit credentials, the S3 client can use each executing worker's configured credential chain.

Credentials in the initiating deployment's resolved storage settings are serialized into workflow payloads and forwarded to every selected Nexus receiver. This includes input credentials in `workflow_input` mode and credentials resolved from plugin configuration or access-key environment variables in `env` mode. A receiver in `env` mode still receives these values before replacing them with its own storage settings. Restrict receivers and access to workers and Temporal histories accordingly. Worker role or profile credentials resolved later by boto3 are not added to the workflow payload by this plugin; see [storage configuration](install_this_plugin.md#configure-targets-and-s3).

A remote deployment in `env` mode resolves its own storage even when the caller supplies settings; the initiating deployment's worker must be able to read that storage if copying into its upload is requested.

## Inspect the results

| Result field | Meaning |
| --- | --- |
| `results` | Mapping from each target key to its execution summary |
| `results.<target>.status` | `SUCCESS` or `FAILED` |
| `results.<target>.is_remote` | Whether the target used Nexus |
| `results.<target>.num_entries_exported` | Exported count for that target |
| `results.<target>.remote_uri` | Download URL or S3 URI on success; use only after checking status |
| `results.<target>.error_message` | Target failure details, when present |
| `total_entries_exported` | Sum across targets, without deduplication |
| `remote_uri` | Primary URI, preferring output from the initiating deployment's `local` target; inspect per-target results for usable locations |
| `workflow_duration` | Total elapsed seconds |

A remote target can report `FAILED` while other targets succeed and the parent workflow completes. A failure of the `local` target can fail the parent workflow. Copying into an upload on the initiating deployment is part of each target's execution and can fail after its S3 output was written; inspect logs as well as results.

Successful S3 ZIPs or prefixes contain the same data artifact, manifest, and metadata as [Export Entries](export_entries.md#retrieve-and-inspect-the-export). ZIP output normally returns a presigned download URL with a requested lifetime of two days (subject to credential expiry), falling back to an `s3://` URI if signing fails. Directory output returns an `s3://` prefix. Zero-match targets still write metadata and an empty manifest, without a data artifact. If copying was requested, find the copied ZIPs or directories in the destination project's files on the initiating deployment. The action returns storage locations rather than upload paths.
