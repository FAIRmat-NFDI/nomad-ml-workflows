# Install and configure the plugin

This guide covers installing and configuring the plugin in an existing NOMAD environment. For Temporal and CPU action-worker setup, follow [Enabling NOMAD Actions in the distribution template](https://github.com/FAIRmat-NFDI/nomad-distro-template#enabling-nomad-actions).

## Installation

### NOMAD deployment

The plugin requires `nomad-lab>=2.0.0`. Add it to your distribution following the [NOMAD Oasis plugin configuration guide](https://docs.nomad-lab.eu/howto/oasis/configure.html#plugins). Include the `cpu-action` extra for export dependencies:

```sh
pip install 'nomad-ml-workflows[cpu-action]'
```

The extra supplies PyArrow for tabular output, boto3 for S3, and `nomad-forces-export` for atomic data. The base installation provides the schema packages and action registration. Configure the plugin in your deployment's `nomad.yaml` as described below.

### NOMAD distro dev

Use [nomad-distro-dev](https://github.com/FAIRmat-NFDI/nomad-distro-dev/) for an integrated development installation. Follow its setup instructions to add this plugin under `packages/nomad-ml-workflows` and register `nomad-ml-workflows[cpu-action]` as a workspace dependency. Apply the plugin settings below in the workspace's root `nomad.yaml`.

For plugin tests, linting, and documentation previews, see [Contribute to this plugin](contribute_to_this_plugin.md).

## Enable entry points

The plugin registers these entry points:

| Entry point | Purpose |
| --- | --- |
| `nomad_ml_workflows.actions:export_entries` | Archive export from this deployment |
| `nomad_ml_workflows.actions:export_forces` | Atomic structures and properties |
| `nomad_ml_workflows.actions:export_remote_entries` | Nexus and S3 export |
| `nomad_ml_workflows.schema_packages:dataset_schema` | Dataset schema |
| `nomad_ml_workflows.schema_packages:model_schema` | ML model schema |

Ensure the required entry points are included in your deployment's plugin selection and are not excluded. Configure action options under `plugins.entry_points.options` as below. Confirm the actions are offered by the GUI or authenticated `GET /actions/schemas` relative to the deployment's API base URL.

## Set limits and action access

Merge this example into the existing `nomad.yaml`, replacing the example user and group IDs:

```yaml
plugins:
  entry_points:
    options:
      nomad_ml_workflows.actions:export_entries:
        max_entries_export_limit: 100000
        read_archives_timeout: 7200
        write_tabular_timeout: 7200
        max_write_buffer_size_bytes: 67108864
        max_write_buffer_size_rows: 1024
        users: [EXPORT_USER_ID]
        groups: [EXPORT_GROUP_ID]
      nomad_ml_workflows.actions:export_forces:
        max_entries_export_limit: 100000
        export_archives_timeout: 10800
        users: [EXPORT_USER_ID]
        groups: [EXPORT_GROUP_ID]
```

Limits must be positive; `0` does not mean unlimited. The shared manifest activity uses the Export Entries cap, including for forces and remote exports. Forces also validates its input against its own cap; keep the two caps aligned when you want the same limit. A remote search applies the executing deployment's cap separately for each target.

Timeouts are in seconds. The two tabular buffer limits control buffered rows and encoded input bytes, not a hard process-memory ceiling; one oversized row can exceed the byte target.

`users` and `groups` contain NOMAD IDs and restrict action submission through the GUI and HTTP API. A user is allowed if their ID is listed **or** they belong to a listed group. Empty or unset restrictions allow authenticated users to start the action. Entry permissions still apply to searches. Remote export defaults to `users: [admin]`; replace it with intended IDs, or set `users: []` and a nonempty `groups` list for group-only access.

## Configure remote export

Here, **local** means the initiating deployment: the current NOMAD deployment from which the action is started. Its `local` target searches that deployment, and optional upload copying saves datasets there.

Enable the remote action and its Nexus handler on the receiving deployment as well as the initiating one. Use Nexus endpoints already available in your deployment. The receiving deployment must recognize the forwarded initiating user's ID.

### Configure Nexus trust and payload encryption

Restrict access to the receiving Nexus endpoint to trusted initiating deployments using your Temporal authentication and authorization setup. Incoming Nexus calls start the export workflow directly and do not recheck the receiving action's `users` or `groups` lists. The receiver trusts the caller to forward the authenticated user's ID; extraction then applies that identity's entry permissions. Configuring a receiver's action allowlists does not restrict incoming Nexus calls.

Configure the same Temporal payload encryption key and key ID in the API and CPU worker environments of all participating deployments. Merge this into each deployment's existing `nomad.yaml`, replacing the placeholder with the same securely supplied secret of at least 32 characters:

```yaml
temporal:
  payload_codec_key: "<shared-secret-at-least-32-characters>"
  payload_codec_key_id: nomad-export-federation
```

Keep each deployment's `services.api_secret` separate. Without an explicit payload key, production clients encrypt using their own API secret, so independently configured deployments cannot decode each other's Nexus payloads. Development clients otherwise leave payloads unencrypted; configure the shared key explicitly when communicating with encrypted peers.

### Configure targets and S3

Merge this configuration on the initiating deployment:

```yaml
plugins:
  entry_points:
    options:
      nomad_ml_workflows.actions:export_remote_entries:
        users: [EXPORT_USER_ID]
        groups: [EXPORT_GROUP_ID]
        local_display_name: Local Oasis
        nexus_endpoints:
          oasis-b:
            display_name: Oasis B
            endpoint: oasis-b
        s3_mode: env
        s3_bucket: ml-datasets
        s3_prefix: exports
        s3_region: eu-central-1
```

Use the **same name** for the mapping key (`oasis-b`), the `endpoint` value, and the registered Nexus endpoint. Current dispatch uses the selected mapping key as the endpoint name. `local` is reserved for execution on the initiating deployment. The display name is only a GUI label.

The default `s3_mode: env` resolves storage on each executing deployment. Set an accessible bucket and credentials on each deployment; configuring the initiating worker alone does not configure a receiver. For optional copying back to an upload on the initiating deployment, its worker must also be able to read the receiver's storage, using a compatible endpoint and credentials.

For ZIP copy-back, configure the same bucket name on the initiating and receiving deployments. ZIP exports normally return a presigned URL, and the current copy implementation parses that URL using the initiating deployment's configured bucket. Read access to a different receiver bucket alone is insufficient. Directory exports return an `s3://` URI that preserves the receiver's bucket name, so those buckets can differ if the initiating worker has the required storage access.

| Entry point option | Environment fallback |
| --- | --- |
| `s3_bucket` | `S3_BUCKET`, `AWS_S3_BUCKET`, `AWS_BUCKET` |
| `s3_prefix` | `S3_PREFIX`, `AWS_S3_PREFIX` |
| `s3_endpoint_url` | `S3_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` |
| `s3_region` | `S3_REGION`, `AWS_DEFAULT_REGION`, `AWS_REGION` |
| `s3_access_key_id` | `S3_ACCESS_KEY_ID`, `AWS_ACCESS_KEY_ID` |
| `s3_secret_access_key` | `S3_SECRET_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY` |
| `s3_session_token` | `S3_SESSION_TOKEN`, `AWS_SESSION_TOKEN` |

Entry point values take precedence over the fallbacks, which are checked in the listed order. Set `s3_endpoint_url` for an S3-compatible service. Workers need permission to write export objects; copying ZIPs requires object reads, and copying directories also requires listing the export prefix.

The initiating deployment forwards its resolved `storage_settings`, including any credential fields, to every selected Nexus receiver through Temporal payloads. These settings come from workflow input in `workflow_input` mode or plugin configuration and the listed environment variables in `env` mode. This forwarding also happens when a receiver uses `s3_mode: env` and replaces those settings before exporting. Only configure trusted receivers, and restrict access to workers and Temporal payloads and histories on both initiating and receiving deployments.

When explicit credential fields are unset, boto3 can resolve credentials locally through each executing worker's configured role or profile. Credentials resolved later by boto3 are not added to the workflow payload by this plugin. Setting the listed access-key environment variables populates the credential fields and therefore forwards those values to receivers.

Alternatively, set `s3_mode: workflow_input` to show and require `storage_settings` in the action form. Configure participating receivers the same way if they should use the caller's supplied settings. A receiver in `env` mode replaces them with its own deployment settings. See the [remote guide](export_remote_entries.md#storage-options) for an input example.

## Verify the deployment

Run the small [Export Entries tutorial](../tutorial/tutorial.md), check the action instance completes, and inspect the ZIP in its destination upload. For remote setup, start with a single configured target and a small limit, check its per-target status and storage location, then enable copying into an upload on the initiating deployment if needed. Search and export counts are explained in the [export overview](../explanation/export_entries.md).
