# ML Models in NOMAD

## Overview

This document describes the design for adding ML model support to NOMAD through
the `nomad-ml-workflows` plugin. The design introduces a dedicated `MLModel`
entry type together with parsing and inference support for ONNX-based model
artifacts.

The objective is to make ML models:

- representable as structured NOMAD entries
- searchable in a dedicated search app
- reusable across uploads and workflows
- linkable to training artifacts, datasets, and downstream analysis
- executable for inference through a controlled NOMAD Action

```mermaid
flowchart LR
    A[User uploads .onnx file] --> B[Parse ONNX]
    B --> C[Create MLModel entry]
    C --> D[Index in search app]
    C --> E[Run inference Action]
    E --> F[Create result entry]
```

## Context

NOMAD already manages structured scientific data, workflows, and provenance.
ML models are increasingly used as scientific artifacts in their own right and
need comparable support:

- a stable schema for metadata and artifact references
- a searchable representation in the NOMAD UI
- a safe way to derive structured metadata from uploaded model files
- a reusable interface for inference-oriented workflows

The current design focuses on ONNX because it offers a practical and relatively
safe path for both metadata extraction and runtime execution without depending
on framework-specific checkpoint loading.

## Goals

- Define a NOMAD-native `MLModel` schema for model entries
- Make `MLModel` entries searchable through a dedicated search app
- Provide automated parsing of `.onnx` files into `MLModel` entries
- Provide a generic inference Action for `MLModel` entries backed by `.onnx`
artifacts

## Non-Goals

- Establish a universal metadata standard for ML models
- Support parsing or inference for PyTorch, TensorFlow, JAX, or scikit-learn
artifacts
- Implement explicit training workflows for ML models inside this feature
- Infer complete provenance automatically from model contents
- Implement import/export integrations for Hugging Face or similar registries

## `MLModel` schema

`MLModel` should be implemented as a top-level NOMAD schema entry using `Entity` and `Schema`.

The schema should separate three concerns:

- user-authored descriptive metadata
- artifact-level technical metadata extracted from files
- execution-related metadata required for inference

Recommended content of the schema:

- Core metadata
    - title or model name
    - description
    - task
    - framework
    - framework version
    - architecture or model family
    - model version
- Artifact metadata
    - reference to one or more model files
    - artifact format
    - checksum
    - file size
- Inference metadata
    - whether inference is available (`.onnx` based entries should have this)
    - runtime type (`.onnx` based entries should have `onnxruntime`)
    - input tensor summary
    - output tensor summary
- Linkage metadata
    - references to training datasets
    - references to workflows or related entries
    - references to evaluation results

Design rules:

- The schema remains NOMAD-native and is not a direct copy of an external ML
metadata standard.
- Missing metadata is preferable to guessed metadata.
- The schema must support manual entry creation even when no parser is used. Model entries can in principle be created manually for non-`.onnx` artifacts by uploading a populated `archive.json` file along with the artifacts, or by using the ELN functionality.

## Search App configurations

The search app should make `MLModel` entries easy to discover and compare.

Search behavior should support the main usage patterns:

- finding reusable models for downstream analysis
- identifying ONNX-backed models that can be executed directly
- filtering models that contain links to training or evaluation context

Recommended filters:

- task
- framework
- architecture or model family
- artifact format
- inference availability
- linked training metadata

Recommended columns:

- model name
- task
- framework
- artifact format
- inference available
- creation time or upload time

## ONNX: why support it?

ONNX is a strong first target because it serves both portability and inference.

Reasons to support ONNX first:

- It can represent models exported from multiple ML frameworks.
- It is better suited for safe ingestion than native checkpoint formats (from PyTorch, Tensorflow) that may rely on unsafe unpickling.
- It provides a common inference-oriented representation across otherwise
heterogeneous training stacks.

Important limitations:

- ONNX is not the canonical source for retraining a model. Exporting into ONNX usually does not preserve optimizer state, scheduler state, or framework-specific checkpoint semantics.

The design therefore treats ONNX primarily as:

- a metadata source for safe parser-based entry creation
- an interchange artifact
- an inference artifact

## ONNX parsing

Parsing can be implemented in two different ways:

- Option A: as an Action that is run at the upload level on-demand by the user. Parses all (or some based on the implementation) the `.onnx` files in the upload.
    - Action can run on `cpu` task queue, not competing with day-to-day processing in NOMAD.
- Option B: as an `ElnMatchingParser` that matches and parses `.onnx` files automatically when uploaded to the upload.
    - Runs on `internal` task queue, parsing will compete with general NOMAD processing. If the memory limits are hit for a large model, the `internal` worker will crash.

Expected parser responsibilities:

- inspect `.onnx` files without executing model code using `onnx.checker.check_model`
- extract basic artifact and graph metadata
- create or overwrite an `MLModel` entry with parser-derived information

Fields that should be populated when available:

- file reference
- artifact format set to ONNX
- checksum
- file size
- model graph name
- input tensor names, shapes, and dtypes
- output tensor names, shapes, and dtypes
- parser notes if metadata is incomplete or partially unavailable

Parsing rules:

- The parser must not attempt to reconstruct training provenance from the ONNX
graph alone.
- The parser must not guess semantic metadata such as scientific task or model
purpose if it is not explicitly present.
- User-supplied metadata should take precedence over parsed data in certain cases.

Resulting user flow:

- user uploads one or more `.onnx` files
- user triggers the parse Action for the upload (option A) or files are automatically parsed (option B)
- parsing creates `MLModel` entries
- entries become searchable and usable for inference
- user supplements the entries with additional metadata

## ONNX inference runtime

Inference should be exposed as a NOMAD Action operating on an existing `MLModel` entry that references an ONNX artifact. Optionally, it should be possible to trigger this Action from the model entry.

Expected Action responsibilities:

- validates the model
- inspect `.onnx` artifacts without executing model code using checksum and `onnx.checker.check_model`
- validate the inputs
- run the inference using `onnxruntime`
- create or overwrite entries with inference results

Inputs to the Action:

- model entry
- inputs for inference
    - key-value pairs - this can be looked up by the action to find any relevant inference settings
    - ordered list, or
    - references to existing entries / sections

Input validation against the model input layer should include required input presence, dtype compatibility, shape compatibility, including dynamic dimensions where applicable.

Output handling should capture raw inference, reference to the model used, and the execution time.

Design constraints:

- inference is only enabled for entries with a valid ONNX artifact
- runtime execution must fail clearly when the artifact or inputs are invalid
- the first version should prioritize a generic execution path over
model-specific pre-processing or visualization

## Interfaces

- Create entry
    - Users can create an `MLModel` entry manually and fill in metadata without
    parser support.
- Parse `.onnx` file
    - Option A: Users can trigger an Action that parses all `.onnx` files in an upload into
    `MLModel` entries.
    - Option B: User can upload `.onnx` files and a `ElnMatchingParser` processes them
    - TBD which option to go with!
- Search App
    - Users can search, filter, and inspect available ML model entries.
- Run inference
    - Users can trigger an Action that runs inference for a selected ONNX-backed
    model using provided inputs.

These interfaces are consistent with the NOMAD model of uploads/projects,
entries, and Actions, so the new feature fits naturally into existing workflows.

## Code ownership

- `nomad-FAIR`
    - provides the generic Action and workflow infrastructure used by the plugin (already available)
    - support for referencing or selecting entries in Action forms (implementation TBD)
    - support for triggering specific Action from inside an Entry (implementation TBD)
- `nomad-ml-workflows`
    - owns the `MLModel` schema
    - owns ONNX parsing implementation
    - owns ONNX inference Action
    - owns the ML model search app configuration

## Docs ownership

- NOMAD main documentation should provide a short overview of ML model support
and link to plugin-specific documentation.
- `nomad-ml-workflows` documentation should own the detailed schema, parsing,
inference, and usage guidance.
