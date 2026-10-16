# ML model schema

`nomad_ml_workflows.schema_packages.model.MLModel` describes a machine learning model independently of its implementation library and artifact format. It is a NOMAD schema that can be used as an entry's `data` section and edited in the ELN. It records the model and its provenance; it does not load model weights, run training, or calculate evaluation scores.

## Model identity and implementation

Alongside the inherited Entity metadata, such as `name`, MLModel provides:

| Field | Meaning |
| --- | --- |
| `architecture` | Architecture or model family, such as `resnet` or `U-Net` |
| `model_version` | Your version identifier for this model |
| `license` | License under which the model is available |
| `tasks` | List of intended tasks |
| `tags` | Free-form discovery labels |
| `libraries` | Repeatable records, each with `name` and `version` |

The task and library editors offer suggestions from the Hugging Face model-tag catalog. These suggestions are optional UI metadata: a temporary catalog/network failure leaves them empty without preventing the schema from loading. Recording a model does not require uploading it to Hugging Face.

## Artifacts and normalization

`artifacts` is repeatable, so one model can refer to multiple files, such as weights and a separate model representation. Each artifact has `model_file` (a path relative to the upload's raw files), `format`, `file_size` in bytes, and `checksum`.

During normalization, an existing artifact file supplies its size, replacing any previously recorded `file_size`. If `format` is missing, the final file extension supplies it in lowercase: `weights.PT` yields `pt`. An explicitly supplied format is preserved. A missing file causes a warning and does not supply size or format.

Checksums are supplied metadata: normalization does not calculate or verify them. Record the algorithm with the value, for example using a `sha256:` prefix. Evaluation results are also supplied metadata and are not recomputed.

## Training and evaluation

The single `training` subsection records `epochs`, `batch_size`, a `dataset` reference, and `dataset_split` (for example, `train`). Its optional `optimizer` subsection records `name` and `learning_rate`.

`evaluations` is repeatable. Each record holds one `metric_name`/`metric_value` pair, a `dataset` reference, and a `dataset_split`, such as `test`. Use separate records for different metrics, datasets, or splits, and make metric names precise enough to explain their scale and units.

The related `nomad_ml_workflows.schema_packages.dataset.Dataset` schema describes datasets using `references` to NOMAD sections/entries, `files` containing raw-file paths, and/or external resource `urls`. Training and evaluation references point to a Dataset section; `dataset_split` labels the portion used without creating or enforcing that split. An [export action](../how_to/use_this_plugin.md) produces files, which you can describe in a Dataset entry yourself.

## Small archive example

Save the following as `model.archive.yaml` in a NOMAD upload with the model schema enabled:

```yaml
data:
  m_def: nomad_ml_workflows.schema_packages.model.MLModel
  name: Example image classifier
  architecture: resnet
  model_version: "1.0"
  license: MIT
  tasks: [image-classification]
  tags: [example]
  libraries:
    - name: pytorch
      version: "2.5"
  artifacts:
    - model_file: models/weights.pt
      format: pytorch-state-dict
  training:
    epochs: 20
    batch_size: 32
    dataset_split: train
    optimizer:
      name: Adam
      learning_rate: 0.001
  evaluations:
    - metric_name: Accuracy (fraction)
      metric_value: 0.91
      dataset_split: test
```

Upload your actual weights at `models/weights.pt` to let normalization record their size. The explicit format remains `pytorch-state-dict`. Supply an actual checksum if available; the example leaves it unset. The evaluation value illustrates manually recorded metadata.

To connect provenance, create a separate Dataset entry and use its reference in `training.dataset` and each applicable `evaluations[].dataset` field through the reference editor. For example, a Dataset's own `data` can be:

```yaml
data:
  m_def: nomad_ml_workflows.schema_packages.dataset.Dataset
  name: Classifier data
  files:
    - datasets/images.zip
```

Replace this raw-file path with your dataset's location in its upload. Use `references` for NOMAD archive sections or `urls` for externally hosted data when those better describe your dataset.
