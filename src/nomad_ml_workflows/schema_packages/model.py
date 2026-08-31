import os
from typing import Any

from huggingface_hub import ModelCard
from nomad.datamodel.data import ArchiveSection, Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.datamodel.metainfo.basesections.v1 import Entity, SectionReference
from nomad.metainfo import Package, Quantity, Section, SubSection
from nomad.metainfo.data_type import Any as AnyType

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


def _read_model_card(archive, model_card_file: str) -> str:
    if os.path.basename(model_card_file).lower() != 'readme.md':
        raise ValueError('The model card file must be named README.md.')

    context = getattr(archive, 'm_context', None)
    if context is None:
        raise ValueError('The model card cannot be read without an archive context.')

    try:
        with context.raw_file(model_card_file, 'rb') as model_card_stream:
            content = model_card_stream.read()
    except (OSError, KeyError) as error:
        raise ValueError(f'Could not read the model card: {error}') from error

    if isinstance(content, bytes):
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError as error:
            raise ValueError(f'The model card is not valid UTF-8: {error}') from error
    if isinstance(content, str):
        return content
    raise ValueError('The model card is not a text file.')


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


class TrainingMetadata(ArchiveSection):
    """Minimal framework-independent metadata describing model training."""

    m_def = Section(label='Training metadata')

    optimizer = Quantity(
        type=str,
        description='The optimizer or optimization algorithm used for training.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    learning_rate = Quantity(
        type=float,
        description='The learning rate used for training, when represented by one value.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity),  # type: ignore
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


class TrainingDataset(SectionReference):
    """A dataset used to train the model, referenced by entry or raw file."""

    m_def = Section(label='Training dataset')

    dataset_file = Quantity(
        type=str,
        description='A training dataset file stored as a raw file.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )


class HuggingFaceModelCardEvaluation(ArchiveSection):
    """A flattened evaluation result from Hugging Face ``model-index`` metadata."""

    m_def = Section(label='Model card evaluation result')

    model_name = Quantity(type=str)
    task_type = Quantity(type=str)
    task_name = Quantity(type=str)
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
    metric_type = Quantity(type=str)
    metric_name = Quantity(type=str)
    metric_value = Quantity(type=AnyType())
    source_name = Quantity(type=str)
    source_url = Quantity(type=str)


def _model_index(model_card: ModelCard) -> list[HuggingFaceModelCardEvaluation]:
    evaluations = []
    for result in model_card.data.eval_results or []:
        evaluations.append(
            HuggingFaceModelCardEvaluation(
                model_name=model_card.data.model_name,
                task_type=result.task_type,
                task_name=result.task_name,
                dataset_type=result.dataset_type,
                dataset_name=result.dataset_name,
                dataset_config=result.dataset_config,
                dataset_split=result.dataset_split,
                dataset_revision=result.dataset_revision,
                metric_type=result.metric_type,
                metric_name=result.metric_name,
                metric_value=result.metric_value,
                source_name=result.source_name,
                source_url=result.source_url,
            )
        )
    return evaluations


class HuggingFaceModelCard(ArchiveSection):
    """Supported metadata imported from a Hugging Face model card README.

    Find the full list of supported fields at https://huggingface.co/docs/hub/model-cards.
    """

    m_def = Section(label='Hugging Face model card')

    model_card_file = Quantity(
        type=str,
        description='A Hugging Face model card README.md stored as a raw file.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )
    language = Quantity(
        type=str,
        shape=['*'],
    )
    license = Quantity(type=str)
    license_name = Quantity(type=str)
    license_link = Quantity(type=str)
    library_name = Quantity(type=str)
    tags = Quantity(type=str, shape=['*'])
    # pipeline_tag = Quantity(type=str)
    # model_name = Quantity(type=str)
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
    # base_model_relation = Quantity(type=str)
    # new_version = Quantity(type=str)
    markdown_content = Quantity(
        type=str,
        description='The Markdown body without its YAML front matter.',
    )
    normalization_status = Quantity(
        type=str,
        description='The result of the most recent model card normalization.',
    )
    normalization_notes = Quantity(type=str, shape=['*'])
    derived_fields = Quantity(
        type=str,
        shape=['*'],
        description='Inherited MLModel fields populated from the model card.',
    )
    model_index = SubSection(
        sub_section=HuggingFaceModelCardEvaluation,
        repeats=True,
        description='Structured evaluation results imported from model-index metadata.',
    )

    def _clear_derived_metadata(self):
        for field_name in _MODEL_CARD_METADATA_FIELDS:
            setattr(self, field_name, None)
        self.model_index = []
        self.normalization_status = None
        self.normalization_notes = []

    def _normalization_error(self, logger, message: str):
        self.normalization_status = 'error'
        self.normalization_notes = [message]
        logger.warning('could_not_normalize_hugging_face_model_card', error=message)

    def _populate_metadata(self, model_card: ModelCard, notes: list[str]):
        data = model_card.data
        for field_name in _MODEL_CARD_SCALAR_FIELDS:
            setattr(
                self,
                field_name,
                _string_value(getattr(data, field_name, None), field_name, notes),
            )
        for field_name in _MODEL_CARD_LIST_FIELDS:
            setattr(
                self,
                field_name,
                _string_list_value(getattr(data, field_name, None), field_name, notes),
            )

        self.model_index = _model_index(model_card)
        if self.model_name is None and self.model_index:
            self.model_name = self.model_index[0].model_name

        metadata = data.to_dict()
        if not metadata:
            notes.append('No Hugging Face model card metadata was found.')
            return

        unsupported_keys = sorted(
            str(key) for key in metadata if key not in _SUPPORTED_MODEL_CARD_KEYS
        )
        if unsupported_keys:
            notes.append(
                'Ignored unsupported model card metadata fields: '
                + ', '.join(unsupported_keys)
                + '.'
            )

    def _normalize_file(self, archive, logger):
        try:
            content = _read_model_card(archive, self.model_card_file)
        except ValueError as error:
            self._normalization_error(logger, str(error))
            return

        try:
            model_card = ModelCard(content, ignore_metadata_errors=False)
        except Exception as error:
            self._normalization_error(
                logger, f'Hugging Face model card validation failed: {error}'
            )
            return

        notes = []
        self.markdown_content = model_card.text.strip()
        self._populate_metadata(model_card, notes)
        self.normalization_status = 'partial' if notes else 'success'
        self.normalization_notes = notes

    def normalize(self, archive, logger):
        self._clear_derived_metadata()
        if self.model_card_file:
            self._normalize_file(archive, logger)
        super().normalize(archive, logger)


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
        sub_section=TrainingMetadata,
        description='Framework-independent metadata describing model training.',
    )
    training_datasets = SubSection(
        sub_section=TrainingDataset,
        repeats=True,
        description=(
            'Datasets used to train the model, referenced as NOMAD archive sections '
            'or raw files.'
        ),
    )
    evaluation_results = SubSection(
        sub_section=SectionReference.m_def,
        repeats=True,
    )


class HuggingFaceMLModel(MLModel):
    """An ML model whose generic metadata is derived from a model card."""

    m_def = Section(label='Hugging Face machine learning model')

    model_card = SubSection(
        sub_section=HuggingFaceModelCard,
        description='Metadata and Markdown imported from a Hugging Face model card.',
    )

    def _clear_derived_fields(self, archive):
        if self.model_card is None:
            return

        derived_fields = list(self.model_card.derived_fields or [])
        previous_name = self.name
        for field_name in derived_fields:
            if field_name in self.m_def.all_quantities:
                setattr(self, field_name, None)
        self.model_card.derived_fields = []

        metadata = getattr(archive, 'metadata', None)
        if (
            'name' in derived_fields
            and metadata is not None
            and metadata.entry_name == previous_name
            and metadata.mainfile
        ):
            metadata.entry_name = os.path.basename(metadata.mainfile)

    def _set_derived_field(self, field_name: str, value: Any):
        if self.model_card is None:
            return
        setattr(self, field_name, value)
        derived_fields = list(self.model_card.derived_fields or [])
        if field_name not in derived_fields:
            derived_fields.append(field_name)
        self.model_card.derived_fields = derived_fields

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
            if value is not None:
                self._set_derived_field(field_name, value)

    def normalize(self, archive, logger):
        self._clear_derived_fields(archive)
        self._populate_generic_fields()
        super().normalize(archive, logger)


m_package.__init_metainfo__()
