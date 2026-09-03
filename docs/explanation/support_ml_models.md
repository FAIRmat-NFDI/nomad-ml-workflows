# Adding support for ML Models in NOMAD

This document defines the foundation for supporting ML models in NOMAD. The
goal here is to treat an ML model as a structured object with descriptive
metadata, technical characteristics, links to training or evaluation data, and
relationships to the workflow that produced or uses it. These features will
allow to search, interpret, compare, and reuse a model.

## Context

NOMAD already manages scientific data, workflows, and provenance in a
structured and FAIR-compliant way. As machine learning becomes a routine part
of scientific practice, trained models and the workflow context around them
need the same level of structured representation.

## Goals

- An extendible base schema for ML model
- Search App for ML models
- Automated parsing of model artifacts into ML model entries for `.onnx` files
- Using a model entry for inference in NOMAD

## Non-Goals

- Automatic ingestion of `.pt`, `.pth`, `.pkl`, `.joblib`, TensorFlow checkpoints, or similar formats
- Supporting PyTorch, TensorFlow, scikit-learn, or other runtime frameworks
- inferred training provenance without explicit references
- export implementations for Hugging Face Model Cards, Croissant, or RO-Crate

## MLModel Schema

`MLModel` should be implemented as a NOMAD `Entity` and `Schema`. It is the primary top-level entry used to represent a model artifact together with user-provided metadata, safely extracted file metadata, and explicit provenance references.

The schema should be NOMAD-native first. External standards are design references, not source schemas to be copied directly into NOMAD.

### MLModel

The `MLModel` section is the top-level model entry.

Required metadata:

- model name or title
- task
- one or more model artifacts

Optional metadata:

- description
- library
- model family or architecture name
- version string
- architecture details
- optimization details
- metrics
- training data references
- workflow references

Deferred metadata:

- rich deployment metadata
- registry synchronization metadata
- hub-specific publication metadata

### ModelArtifact

This section captures raw model artifact information. It should be the canonical location for file-level metadata.

Required metadata:

- raw file path
- artifact format
- checksum
- file size

Optional metadata:

- parser notes
- safe metadata extraction status
- tensor inventory summary

Deferred metadata:

- artifact bundles spanning multiple files
- framework-specific restore instructions
- execution or deployment configuration

### ArchitectureDetails

This section captures structural metadata about the model when such metadata can be obtained safely.

Optional metadata:

- library
- model family
- architecture name
- input signature summary
- output signature summary
- tensor summary

Important rule:

- if the artifact does not contain trustworthy architecture metadata, the parser should leave these fields empty rather than guess.

### OptimizationDetails

This section captures training or optimization metadata only when it is explicitly available.

Optional metadata:

- optimizer name
- optimization framework
- selected optimization settings

Deferred metadata:

- detailed trainer state
- scheduler internals
- executable checkpoint restore state

### Metric

This section captures scalar performance metadata associated with the model.

Optional metadata:

- metric name
- split such as training, validation, or test
- metric value
- optional unit
- optional description

Deferred metadata:

- full metric histories
- benchmark suite integration

### TrainingDataReference

This section captures explicit provenance references used to train or evaluate the model.

Optional metadata:

- internal references to NOMAD entries
- file references to uploaded dataset artifacts
- optional description of how the referenced data was used

Important rule:

- provenance is only recorded from explicit references. It is not inferred from model contents.

## Standards and Design References

No single open standard covers the full NOMAD use case for scientific ML models. The current design therefore uses external standards as references rather than as source schemas.

- **OpenMetadata `MlModel`**
  - useful for field ideas, entity-level metadata, and future crosswalks
  - not adopted directly as the NOMAD schema

- **ML-Schema**
  - useful as a semantic reference for tasks, trained models, datasets, and training concepts
  - not used as a direct field model in the current design

- **RO-Crate and W3C PROV**
  - useful for future provenance packaging and export
  - not part of the current implementation

- **MLCommons Croissant**
  - useful as a future export target for linked dataset metadata
  - not treated as the internal NOMAD source model

In practice, the current design should:

- use a NOMAD-native `MLModel` schema
- borrow selected concepts from OpenMetadata where they map cleanly
- use ML-Schema as a semantic reference only
- leave RO-Crate, PROV, and Croissant to later interoperability work

## Ingestion and Parsing

Model ingestion is part of ML model support, but the current design intentionally supports automatic parsing for only one format: `safetensors`.

- `safetensors` files are parsed automatically and generate an `MLModel` entry
- unsupported formats do not get automatic parser support
- unsupported formats may still be represented manually by the user as `MLModel` entries

### Parsing Policy

The parser is metadata-only.

It must:

- inspect the artifact without executing model code
- populate file metadata required for the `ModelArtifact` section
- extract only metadata that can be read safely and deterministically

It may:

- populate library, model family, or architecture fields if trusted metadata is present in the file
- populate tensor inventory summaries such as tensor names, shapes, counts, and dtypes when these can be read safely

It must never:

- unpickle Python objects
- call `torch.load`
- call `joblib.load`
- invoke TensorFlow restore logic
- execute framework-specific model constructors
- run model inference

The current design explicitly prefers incomplete-but-truthful metadata over heuristic guesses.

### Fields the Parser Must Populate

For `safetensors`, the parser must populate:

- artifact path
- artifact format
- checksum
- file size

### Fields the Parser May Populate

When trusted metadata is available, the parser may also populate:

- library
- model family or architecture name
- tensor inventory summary
- parser notes about extracted metadata

If these fields are not present in trusted metadata, they should remain unset.

## Training Provenance in NOMAD

The current design uses a model-centered provenance design.

Training provenance is attached primarily to the `MLModel` entry through explicit references rather than through a required dedicated training activity entry.

The model entry may include:

- NOMAD entry references to training data
- file references to uploaded dataset artifacts
- optional references to configuration or auxiliary artifacts
- optional `workflow2` links when the model was produced within an existing NOMAD workflow

The current design does not require a separate entry representing the training activity.

### Provenance Normalization

If the training references include structured NOMAD entries with material information, NOMAD may normalize a collective material summary into `results.material` for the `MLModel` entry.

This normalization should follow two rules:

- it only uses explicit referenced structured entries
- it does not infer materials or provenance from the model artifact itself

If no references are provided, no provenance graph is auto-created.

## ONNX as an Inference Artifact

ONNX is a useful portability and inference format, but it should not be treated as the canonical training artifact for a model entry.

In NOMAD, ONNX is best understood as a derived artifact for:

- portable inference
- deployment-oriented interchange
- framework-independent execution in compatible runtimes

This makes ONNX a strong candidate for representing a model that is intended to be executed outside its original training stack. It is particularly useful when users want a common inference representation across models originally developed in PyTorch, TensorFlow, Keras, or scikit-learn.

At the same time, ONNX has important limitations for retraining workflows.

An ONNX export typically does not preserve the full native training state, such as:

- optimizer state
- scheduler state
- framework-specific checkpoint semantics
- custom training logic
- exact resume-from-checkpoint behavior

As a result, ONNX should be treated as a secondary artifact rather than the source of truth when users need to continue training a model.

The preferred model in NOMAD is:

- native training artifact as the canonical retraining source
- ONNX artifact as a derived inference or interchange artifact
- explicit metadata and provenance stored independently of both

This means that support for ONNX should focus on safe parsing, artifact description, and inference-oriented metadata, while retraining-capable workflows should continue to rely on the native framework artifacts and associated configuration.

There is also an important safety boundary between native PyTorch checkpoints and exported ONNX artifacts.

Loading an ONNX file with standard ONNX tooling is generally much safer than loading a user-supplied PyTorch `.pt` checkpoint, because ONNX is a serialized graph format rather than a Python pickle. In contrast, loading a `.pt` file may trigger unpickling and therefore may execute malicious payloads embedded in the original checkpoint.

This means that:

- a user-supplied ONNX artifact is a safer ingestion target than a user-supplied `.pt` artifact
- converting an uploaded `.pt` file to ONNX on the server is not a safe default, because the risky step is loading the `.pt` file before conversion

As a result, ONNX should not be treated as making the original PyTorch artifact retroactively safe. An exported ONNX model may be appropriate as a safer downstream artifact, but it does not remove the security risk of the original `.pt -> ONNX` conversion step.

## Future Extensions

The schema should be designed so that later interoperability and format support can be added without reshaping the core model entry.

Future work may include:

- Hugging Face Model Card export
- Croissant export for linked dataset metadata
- RO-Crate or PROV-based provenance packaging
- additional safe model formats such as ONNX or selected framework-native formats

Any future automatic ingestion of additional formats should satisfy the same safety standard as the current design:

- no unsafe deserialization
- no execution of embedded or framework-defined code
- metadata extraction must remain deterministic and inspectable

## Final Design Decisions

The accepted design is:

- `MLModel` is a NOMAD-native schema, not a direct import of an external standard
- automatic parsing scope is limited to `safetensors`
- safe parsing means metadata extraction without unpickling or code execution
- unsupported formats are manual-entry-only in the current design
- provenance is attached primarily through references on the model entry
- exports and broader format support are deferred
