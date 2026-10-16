# Install and configure the plugin

This guide is for administrators of an existing NOMAD deployment. Install the plugin in the environments or images used by the API and action workers, and apply matching plugin configuration. For container deployments, build these dependencies into the deployment images. See the [NOMAD Oasis configuration guide](https://docs.nomad-lab.eu/howto/oasis/configure.html#plugins) for deployment-wide plugin configuration.

## Install dependencies and enable entry points

The package requires `nomad-lab>=1.4.3`. Install the worker dependencies with the `cpu-action` extra:

```sh
pip install 'nomad-ml-workflows[cpu-action]'
```

The extra supplies PyArrow for tabular output, boto3 for S3, and `nomad-forces-export` for atomic data. The base installation provides the schema packages and action registration; CPU workers need the extra to execute all export formats.

The plugin registers these entry points:

| Entry point | Purpose |
| --- | --- |
| `nomad_ml_workflows.actions:export_entries` | Local archive export |
| `nomad_ml_workflows.actions:export_forces` | Atomic structures and properties |
| `nomad_ml_workflows.actions:export_remote_entries` | Nexus and S3 export |
| `nomad_ml_workflows.schema_packages:dataset_schema` | Dataset schema |
| `nomad_ml_workflows.schema_packages:model_schema` | ML model schema |

Ensure the required entry points are included in your deployment's plugin selection and are not excluded. Configure action options under `plugins.entry_points.options` as below. Restart the API and CPU workers after installation or configuration changes. Confirm the actions are offered by the GUI or authenticated `GET /actions/schemas` relative to the deployment's API base URL.

## Configure Temporal and start the CPU worker

The API and workers must connect to the same Temporal service and namespace. For a service reachable as `temporal:7233`, add this to `nomad.yaml`:

```yaml
temporal:
  enabled: true
  host: temporal
  port: 7233
  namespace: default
```

Replace the host and namespace with your deployment's values, and configure its required Temporal authentication. Start a CPU action worker in the configured NOMAD environment:

```sh
nomad admin run action-cpu-worker
```

In the `nomad-distro-dev` workspace, the existing `uv run poe cpuworker` task starts this worker with the local configuration. The root `uv run poe start` task starts the development API/app worker; run the CPU worker alongside it.

The CPU worker polls `cpu-task-queue`, where all three export actions run. Submission can succeed without a polling worker, leaving the action waiting to execute. Workers need access to NOMAD's database, search index, archive/raw storage, and shared action-artifact storage. Ensure filesystem paths and permissions are consistent across the workers that may run an action's activities.

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

Timeouts are in seconds. The two tabular buffer limits control buffered rows and encoded input bytes, not a hard process-memory ceiling; one oversized row can exceed the byte target. Optionally set `MIMALLOC_PURGE_DELAY=0` in the CPU worker environment before startup when using PyArrow with mimalloc to release unused pages more promptly.

`users` and `groups` contain NOMAD IDs. A user is allowed if their ID is listed **or** they belong to a listed group. Empty or unset restrictions allow authenticated users to start the action. Entry permissions still apply to searches. Remote export defaults to `users: [admin]`; replace it with intended IDs, or set `users: []` and a nonempty `groups` list for group-only access.

## Configure remote export

Enable the remote action and its Nexus handler on the receiving deployment as well as the initiating one. Each deployment needs an active CPU worker. Their identity setup must recognize the forwarded user ID, and their Temporal infrastructure must permit the Nexus connection.

### Register the Nexus endpoint

For deployments using different namespaces on one Temporal service, register `oasis-b` on that service, routing to the receiving namespace's CPU queue:

```sh
temporal operator nexus endpoint create \
  --name oasis-b \
  --target-namespace oasis-b-namespace \
  --target-task-queue cpu-task-queue

temporal operator nexus endpoint get --name oasis-b
```

Use your CLI connection and authentication settings for that service. Endpoint creation runs once per endpoint name. Separate Temporal services require an externally routed Nexus endpoint and corresponding network/authentication setup; a namespace on another service cannot be reached by the worker-target example alone. See the [Temporal CLI operator reference](https://github.com/temporalio/documentation/blob/main/docs/cli/command-reference/operator.mdx#nexus) for endpoint and connection options.

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

Use the **same name** for the mapping key (`oasis-b`), the `endpoint` value, and the registered Nexus endpoint. Current dispatch uses the selected mapping key as the endpoint name. `local` is reserved for local execution. The display name is only a GUI label.

The default `s3_mode: env` resolves storage on each executing deployment. Set an accessible bucket and credentials on each deployment; configuring the initiating worker alone does not configure a receiver. For optional copying back to a local upload, the initiating worker must also be able to read the receiver's storage, using a compatible endpoint and credentials. Using the same S3 service and accessible bucket across participating deployments is a straightforward setup.

| Entry point option | Environment fallback |
| --- | --- |
| `s3_bucket` | `S3_BUCKET`, `AWS_S3_BUCKET`, `AWS_BUCKET` |
| `s3_prefix` | `S3_PREFIX`, `AWS_S3_PREFIX` |
| `s3_endpoint_url` | `S3_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` |
| `s3_region` | `S3_REGION`, `AWS_DEFAULT_REGION`, `AWS_REGION` |
| `s3_access_key_id` | `S3_ACCESS_KEY_ID`, `AWS_ACCESS_KEY_ID` |
| `s3_secret_access_key` | `S3_SECRET_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY` |
| `s3_session_token` | `S3_SESSION_TOKEN`, `AWS_SESSION_TOKEN` |

Entry point values take precedence over the fallbacks, which are checked in the listed order. If explicit credentials are absent, boto3 can use the worker's configured credential chain. Set `s3_endpoint_url` for an S3-compatible service. Workers need permission to write export objects; copying ZIPs requires object reads, and copying directories also requires listing the export prefix. Credential fields are serialized for workflow execution, including when resolved from the environment, so restrict access to Temporal payloads and history.

Alternatively, set `s3_mode: workflow_input` to show and require `storage_settings` in the action form. Configure participating receivers the same way if they should use the caller's supplied settings. A receiver in `env` mode replaces them with its own deployment settings. See the [remote guide](export_remote_entries.md#storage-options) for an input example.

## Verify the deployment

Run the small [Export Entries tutorial](../tutorial/tutorial.md), check the action instance completes, and inspect the ZIP in its destination upload. For remote setup, start with a single configured target and a small limit, check its per-target status and storage location, then enable local copying if needed. Search and export counts are explained in the [export overview](../explanation/explanation.md).
