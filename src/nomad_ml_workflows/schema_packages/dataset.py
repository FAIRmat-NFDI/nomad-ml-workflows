from nomad.datamodel.data import ArchiveSection, Package, Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.datamodel.metainfo.basesections import Entity
from nomad.metainfo.metainfo import Quantity, Section

m_package = Package(name='Dataset schema')


class Dataset(Entity, Schema):
    """A dataset available as NOMAD sections/entries, raw files, or at a URL.

    TODO: Add croissant file to enhance interoperability support.
    """

    m_def = Section(label='Dataset')

    references = Quantity(
        type=ArchiveSection,
        shape=['*'],
        description='References to NOMAD sections/entries comprising the dataset.',
    )
    files = Quantity(
        type=str,
        shape=['*'],
        description='Paths to local files comprising the dataset.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity),  # type: ignore
    )
    urls = Quantity(
        type=str,
        shape=['*'],
        description='URLs to resources comprising the dataset.',
        a_eln=ELNAnnotation(component=ELNComponentEnum.URLEditQuantity),  # type: ignore
    )


m_package.__init_metainfo__()
