from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from huggingface_hub import ModelCard
from nomad.datamodel.context import Context
from nomad.datamodel.data import ArchiveSection, Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.datamodel.metainfo.basesections import Entity
from nomad.metainfo.metainfo import Package, Quantity, Section, SubSection

from nomad_ml_workflows.schema_packages.dataset import Dataset

if TYPE_CHECKING:
    from structlog import BoundLogger

m_package = Package(name='ML model schema')

_MODEL_CARD_SCALAR_FIELDS = (
    'license',
    'license_name',
    'license_link',
    'library_name',
    'pipeline_tag',
    'model_name',
    'base_model_relation',
    'new_version',
)
_MODEL_CARD_LIST_FIELDS = (
    'language',
    'tags',
    'datasets',
    'metrics',
    'base_model',
)
_MODEL_CARD_METADATA_FIELDS = (
    *_MODEL_CARD_SCALAR_FIELDS,
    *_MODEL_CARD_LIST_FIELDS,
    'markdown_content',
)
_SUPPORTED_MODEL_CARD_KEYS = {
    *_MODEL_CARD_SCALAR_FIELDS,
    *_MODEL_CARD_LIST_FIELDS,
    'model-index',
}


def _string_value(value: Any, key: str, notes: list[str]) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    notes.append(f'Ignored `{key}` because its value is not a string.')
    return None


def _string_list_value(value: Any, key: str, notes: list[str]) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    notes.append(f'Ignored `{key}` because it is not a string or list of strings.')
    return []


def _read_model_card(model_card_file: str, context: Context) -> ModelCard:
    """Reads and parses the model card from the raw file path in the given upload context."""
    if os.path.basename(model_card_file).lower() != 'readme.md':
        raise ValueError('The model card file must be named README.md.')

    try:
        with context.raw_file(model_card_file, 'rb') as fp:
            content = fp.read()
    except (OSError, KeyError) as error:
        raise ValueError(f'Could not read the model card: {error}') from error

    if isinstance(content, bytes):
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError as error:
            raise ValueError(f'The model card is not valid UTF-8: {error}') from error
    if not isinstance(content, str):
        raise ValueError('The model card is not a text file.')

    try:
        return ModelCard(content, ignore_metadata_errors=False)
    except Exception as error:
        raise ValueError(f'Could not parse the model card: {error}') from error


class ModelArtifact(ArchiveSection):
    """A file containing all or part of a machine learning model."""

    m_def = Section(label='Model artifact')

    model_file = Quantity(
        type=str,
        description='A model artifact stored as a raw file.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )
    format = Quantity(
        type=str,
        description='The serialization or packaging format of the artifact.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    checksum = Quantity(
        type=str,
        description='A checksum identifying the exact artifact contents.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    file_size = Quantity(
        type=int,
        unit='byte',
        description='The size of the artifact in bytes.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
    )


class Optimizer(ArchiveSection):
    """The optimizer or optimization algorithm used for training."""

    m_def = Section(label='Optimizer')

    name = Quantity(
        type=str,
        description='The name of the optimizer or optimization algorithm.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    learning_rate = Quantity(
        type=float,
        description='The learning rate used for training, when represented by one value.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
    )


class Training(ArchiveSection):
    """Minimal framework-independent metadata describing model training."""

    m_def = Section(label='Training metadata')

    optimizer = SubSection(
        sub_section=Optimizer,
        description='The optimizer or optimization algorithm used for training.',
    )
    epochs = Quantity(
        type=int,
        description='The number of completed training epochs.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
    )
    batch_size = Quantity(
        type=int,
        description='The number of samples in one training batch.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
    )
    dataset = Quantity(
        type=Dataset,
        description='The dataset used for training.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.ReferenceEditQuantity),  # type: ignore
    )
    dataset_split = Quantity(
        type=str,
        description='The dataset split used for training, for example `train`.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )


class Evaluation(ArchiveSection):
    """A model evaluation result for one task, dataset, split, and metric."""

    m_def = Section(label='Evaluation')

    task_name = Quantity(
        type=str,
        description='The task on which the model was evaluated. For example '
        '`Crystal structure classification`.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    metric_name = Quantity(
        type=str,
        description='The human-readable name of the reported evaluation metric, for example '
        '`Top-5 Accuracy`, `F1`.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    metric_value = Quantity(
        type=float,
        description='The value of the reported evaluation metric.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
    )
    dataset = Quantity(
        type=Dataset,
        description='The dataset used to evaluate the model.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.ReferenceEditQuantity),  # type: ignore
    )
    dataset_split = Quantity(
        type=str,
        description='The dataset split used for evaluation, for example `test`.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )


class HuggingFaceModelCardEvaluation(Evaluation):
    """A flattened evaluation result from Hugging Face ``model-index`` metadata."""

    m_def = Section(label='Model card evaluation')

    model_name = Quantity(type=str)
    task_type = Quantity(
        type=str,
        description='The machine-readable type of the task on which the model was evaluated, '
        'for example `classification`, `regression`.',
    )
    dataset_type = Quantity(type=str)
    dataset_name = Quantity(type=str)
    dataset_config = Quantity(type=str)
    dataset_split = Quantity(type=str)
    dataset_revision = Quantity(type=str)
    dataset_args = Quantity(
        type=str,
        shape=['*', '2'],
        description='key-value pairs for additional arguments to `load_dataset()`',
    )
    metric_type = Quantity(
        type=str,
        description='The machine-readable type of the reported evaluation metric, '
        'for example `accuracy`, `f1`.',
    )
    source_name = Quantity(
        type=str,
        description='The name of the source reporting the evaluation result.',
    )
    source_url = Quantity(
        type=str,
        description='A URL identifying the source of the evaluation result.',
    )
    metric_value = Quantity(type=str)


class HuggingFaceModelCard(ArchiveSection):
    """Supported metadata imported from a Hugging Face model card README.

    Find the full list of supported fields at https://huggingface.co/docs/hub/model-cards
    and https://github.com/huggingface/hub-docs/blob/main/modelcard.md?plain=1
    """

    m_def = Section(label='Hugging Face model card')

    model_name = Quantity(type=str)
    language = Quantity(
        type=str,
        shape=['*'],
    )
    license = Quantity(type=str)
    license_name = Quantity(type=str)
    license_link = Quantity(type=str)
    library_name = Quantity(type=str)
    tags = Quantity(type=str, shape=['*'])
    pipeline_tag = Quantity(type=str)
    datasets = Quantity(
        type=str,
        shape=['*'],
        description='External Hugging Face dataset identifiers.',
    )
    buckets = Quantity(type=str, shape=['*'])
    metrics = Quantity(type=str, shape=['*'])
    base_model = Quantity(
        type=str,
        shape=['*'],
        description='External Hugging Face identifiers for models this model derives from.',
    )
    base_model_relation = Quantity(type=str)
    new_version = Quantity(type=str)
    markdown_content = Quantity(
        type=str,
        description='The Markdown body without its YAML front matter.',
    )


class MLModel(Entity, Schema):
    """A generic, framework- and artifact-format-independent ML model entry."""

    m_def = Section(label='Machine learning model')

    task = Quantity(
        type=str,
        description='The task for which the model is intended.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    framework = Quantity(
        type=str,
        description='The framework or library with which the model is used.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    framework_version = Quantity(
        type=str,
        description='The version of the framework associated with the model.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    architecture = Quantity(
        type=str,
        description='The architecture, model family, or other structural designation.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    model_version = Quantity(
        type=str,
        description='A version identifier for the model.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    license = Quantity(
        type=str,
        description='The license under which the model is made available.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    tags = Quantity(
        type=str,
        shape=['*'],
        description='Free-form labels used to describe and discover the model.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    artifacts = SubSection(
        sub_section=ModelArtifact,
        repeats=True,
        description='Files containing all or part of the machine learning model.',
    )
    training = SubSection(
        sub_section=Training,
        description='Framework-independent metadata describing model training.',
    )
    evaluations = SubSection(
        sub_section=Evaluation,
        repeats=True,
        description='Evaluation results reported for the model.',
    )


class HuggingFaceMLModel(MLModel):
    """An ML model whose generic metadata is derived from a model card."""

    m_def = Section(label='Hugging Face machine learning model')

    model_card_file = Quantity(
        type=str,
        description='A Hugging Face model card README.md stored as a raw file.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )
    model_card = SubSection(
        sub_section=HuggingFaceModelCard,
        description='Metadata and Markdown imported from a Hugging Face model card.',
    )

    def _populate_evaluation_results(self, model_card: ModelCard, logger: BoundLogger):
        """
        Populate evaluation results from Hugging Face ``model-index`` metadata.
        """
        if not model_card.data or not model_card.data.eval_results:
            return

    def _populate_model_card_data(self, model_card: ModelCard, logger: BoundLogger):
        if not model_card.data:
            return

        data = model_card.data.to_dict()
        if not data:
            logger.warning('No Hugging Face model card metadata was found.')
            return

        notes = []
        for field_name in _MODEL_CARD_SCALAR_FIELDS:
            setattr(
                self.model_card,
                field_name,
                _string_value(data.get(field_name, None), field_name, notes),
            )
        for field_name in _MODEL_CARD_LIST_FIELDS:
            setattr(
                self.model_card,
                field_name,
                _string_list_value(data.get(field_name, None), field_name, notes),
            )
        if notes:
            logger.warning('\n'.join(notes))

        unsupported_keys = sorted(
            str(key) for key in data if key not in _SUPPORTED_MODEL_CARD_KEYS
        )
        if unsupported_keys:
            logger.warning(
                f'Ignored unsupported model card metadata fields: {", ".join(unsupported_keys)}.',
            )

        self._populate_evaluation_results(model_card, logger)

    def _normalize_model_card(self, archive, logger):
        if not self.model_card_file:
            return

        try:
            model_card = _read_model_card(self.model_card_file, archive.m_context)
        except ValueError as error:
            logger.warning('failed to read model card', error=str(error))
            return

        self.model_card = HuggingFaceModelCard()

        self.model_card.markdown_content = model_card.text.strip()
        self._populate_model_card_data(model_card, logger)

    def _populate_generic_fields(self):
        if self.model_card is None:
            return

        mappings = {
            'name': self.model_card.model_name,
            'description': self.model_card.markdown_content,
            'task': self.model_card.pipeline_tag,
            'framework': self.model_card.library_name,
            'license': self.model_card.license,
            'tags': self.model_card.tags,
        }
        for field_name, value in mappings.items():
            if value and not getattr(self, field_name):
                setattr(self, field_name, value)

    def normalize(self, archive, logger):
        self._normalize_model_card(archive, logger)
        self._populate_generic_fields()

        super().normalize(archive, logger)


m_package.__init_metainfo__()
