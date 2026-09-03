from nomad.datamodel.data import Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.datamodel.metainfo.basesections import Entity, SectionReference
from nomad.metainfo.metainfo import Quantity, Section, SubSection


class Dataset(Entity, Schema):
    """A dataset available as NOMAD sections/entries, raw files, or at a URL."""

    m_def = Section(label='Dataset')

    references = SubSection(
        sub_section=SectionReference,
        repeats=True,
        description='References to NOMAD sections/entries containing the dataset.',
    )
    files = Quantity(
        type=str,
        shape=['*'],
        description='Dataset stored as raw files.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )
    url = Quantity(
        type=str,
        description='Link to an external resource related to the dataset.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.URLEditQuantity),  # type: ignore
    )
