from __future__ import annotations

from nomad.datamodel.data import ArchiveSection, Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.datamodel.metainfo.basesections import Entity
from nomad.metainfo.metainfo import Package, Quantity, Section, SubSection

from nomad_ml_workflows.hf import libraries as hf_libraries
from nomad_ml_workflows.hf import tasks as hf_tasks
from nomad_ml_workflows.schema_packages.dataset import Dataset

m_package = Package(name='ML model schema')


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
    """Framework-independent metadata describing model training."""

    m_def = Section(label='Training metadata')

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
    optimizer = SubSection(
        sub_section=Optimizer,
        description='The optimizer or optimization algorithm used for training.',
    )


class Evaluation(ArchiveSection):
    """A model evaluation result for one dataset, split, and metric."""

    m_def = Section(label='Evaluation')

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


class MLModel(Entity, Schema):
    """A generic, library- and artifact-format-independent ML model entry."""

    m_def = Section(label='Machine learning model')

    task = Quantity(
        type=str,
        description='The task for which the model is intended. E.g., "image-segmentation".',
        a_eln=ELNAnnotation(
            component=ELNComponentEnum.EnumEditQuantity,
            props={'suggestions': hf_tasks},
        ),  # type: ignore
    )
    library = Quantity(
        type=str,
        description='The library with which the model is implemented. E.g., "pytorch".',
        a_eln=ELNAnnotation(
            component=ELNComponentEnum.EnumEditQuantity,
            props={'suggestions': hf_libraries},
        ),  # type: ignore
    )
    library_version = Quantity(
        type=str,
        description='The version of the library associated with the model.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity),  # type: ignore
    )
    architecture = Quantity(
        type=str,
        description='The architecture, model family, or other structural designation. E.g., "resnet", "U-Net".',
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


m_package.__init_metainfo__()
