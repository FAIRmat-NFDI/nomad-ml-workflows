# nomad-ml-workflows

A NOMAD plugin for exporting datasets and describing machine learning models.

- **Export Entries:** Export complete archives or selected paths as JSON, CSV, or Parquet.
- **Export Forces:** Export atomic structures with energies, forces, and stresses as Extended XYZ or an ASE database.
- **Export Remote Entries:** Export datasets from configured Oases to S3, optionally copying them into a local upload.
- **MLModel and Dataset schemas:** Describe model artifacts, training data, and evaluation metadata.

## Installation

Install the plugin with its export-worker dependencies:

```sh
pip install 'nomad-ml-workflows[cpu-action]'
```

Export actions require a configured NOMAD deployment and a running CPU action worker.

## Documentation

See the [documentation](https://fairmat-nfdi.github.io/nomad-ml-workflows/) for the GUI and REST tutorial, action guides, deployment configuration, and schema examples.

## License

Licensed under the MIT License. See [LICENSE](LICENSE).
