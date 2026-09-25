import json
from typing import Literal

from nomad.app.v1.models.models import Query
from nomad.config import config as nomad_config

try:
    from nomad_forces_export.config import BASE_QUERY
except ImportError:
    BASE_QUERY = {}
from pydantic import BaseModel, ConfigDict, Field, field_validator

from nomad_ml_workflows.actions.export_entries.models import (
    ExportDatasetMetadata,
    OutputFile,
    PrepareManifestInput,
    PrepareManifestOutput,
)

OwnerLiteral = Literal[
    'visible',
    'public',
    'user',
    'shared',
    'staging',
]


config = nomad_config.get_plugin_entry_point('nomad_ml_workflows.actions:export_forces')

DataFileFormatLiteral = Literal['extxyz', 'ase_db']
PropertiesLiteral = Literal['energy', 'forces', 'stress']


def _clean_field(field: str) -> str:
    """
    Removes trailing whitespaces and inverted commas
    """
    return field.strip().strip("'").strip('"')


_DIRECTIVE_PRIORITY = {'include': 1, 'include-resolved': 2, 'exclude': 3}

PropertiesLiteral = Literal['Energy', 'Forces', 'Stress']


class IncludeProperties(BaseModel):
    forces: bool = Field(False, description='Include forces data.')
    energies: bool = Field(False, description='Include energy data.')
    stresses: bool = Field(False, description='Include stress data.')


class ForcesSearchSettings(BaseModel):
    owner: OwnerLiteral = Field(
        'visible',
        title='Ownership scope',
        description='Choose which entries are eligible for export.',
        json_schema_extra={
            'uiSchema': {
                'ui:enumNames': [
                    'All entries visible to me (visible)',
                    'Public entries (public)',
                    'My entries (user)',
                    'My and shared entries (shared)',
                    'My and shared unpublished entries (staging)',
                ],
            }
        },
    )
    max_entries: int = Field(
        min(1000, config.max_entries_export_limit),  # type: ignore
        gt=0,
        le=config.max_entries_export_limit,  # type: ignore
        title='Maximum entries',
        description=(
            'Export at most this many matching entries. The deployment limit is '
            f'{config.max_entries_export_limit}.'  # type: ignore
        ),
    )
    max_frames: int | None = Field(
        None,
        title='Maximum frames',
        description=(
            'Export at most this many frames from the matching entries. '
            'If not specified, all frames will be exported.'
        ),
    )
    query: str = Field(
        json.dumps(BASE_QUERY, indent=2),
        title='Search query',
        description='NOMAD search query written as a JSON object.',
        json_schema_extra={
            'uiSchema': {
                'ui:widget': 'textarea',
                # 'ui:placeholder': '{\n  "entry_type": "ELNSample"\n}',
                'ui:help': (
                    'You can also copy the query from the **View API Call** '
                    'dialog in a NOMAD search app.'
                ),
                'ui:options': {'rows': 5, 'enableMarkdownInHelp': True},
            }
        },
    )
    properties: IncludeProperties = Field(
        ...,
        title='Properties',
        description='Select which properties to include in the export.',
    )

    @field_validator('properties')
    @classmethod
    def check_at_least_one_property(cls, v: IncludeProperties) -> IncludeProperties:
        if not (v.energies or v.forces or v.stresses):
            raise ValueError(
                'At least one of Energies, Forces, or Stresses must be selected in properties.'
            )
        return v

    @property
    def required_properties(self) -> list[str]:
        """
        Returns a list of required properties based on the user's selection.
        """
        required = []
        if self.properties.energies:
            required.append('energy')
        if self.properties.forces:
            required.append('forces')
        if self.properties.stresses:
            required.append('stress')
        return required


class DataFileFormat(BaseModel):
    extxyz: bool = Field(False, description='Export as extxyz file.')
    ase_db: bool = Field(False, description='Export as ASE DB file.')

    @property
    def selected_formats(self) -> list[DataFileFormatLiteral]:
        """
        Returns a list of selected data file formats based on the user's selection.
        """
        selected = []
        if self.extxyz:
            selected.append('extxyz')
        if self.ase_db:
            selected.append('ase_db')
        return selected


class ForcesExportSettings(BaseModel):
    file_format: DataFileFormat = Field(
        ...,
        title='File format',
        description='File format for the exported entry data.',
    )
    create_zip_archive: bool = Field(
        True,
        title='Create ZIP archive',
        description='Bundle all export artifacts into a ZIP archive.',
        json_schema_extra={
            'uiSchema': {
                'ui:help': (
                    'Bundle all export artifacts into a ZIP archive. Turn this '
                    'off to save them as a project subdirectory.'
                ),
            }
        },
    )

    @field_validator('file_format')
    @classmethod
    def check_at_least_one_property(cls, v: DataFileFormat) -> DataFileFormat:
        if not (v.extxyz or v.ase_db):
            raise ValueError(
                'At least one of extxyz or ase_db must be selected in file_format.'
            )
        return v


class ForcesExportEntriesUserInput(BaseModel):
    model_config = ConfigDict(title='')

    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )  # required field that is not shown in the Action Form UI
    upload_id: str = Field(
        ...,
        title='Destination project ID',
        description='ID of the project/upload where the exported artifacts will be saved.',
    )
    search_settings: ForcesSearchSettings = Field(..., title='Search options')
    export_settings: ForcesExportSettings = Field(..., title='Export options')


class ForcesExtractEntriesWorkflowInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_input: ForcesExportEntriesUserInput = Field(
        ..., description='Original user input for the export entries workflow.'
    )


class ForcesNormalizedSearchSettings(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    num_entries_user_limit: int = Field(
        ..., description='Maximum number of entries requested by the user.'
    )

    @classmethod
    def from_user_input(
        cls,
        user_input: ForcesExportEntriesUserInput,
    ) -> 'ForcesNormalizedSearchSettings':
        query = json.loads(
            _clean_field(user_input.search_settings.query).replace("'", '"')
        )
        query.setdefault('quantities:all', []).extend(
            [
                f'run.calculation.{p}.total.value'
                for p in user_input.search_settings.required_properties
            ]
        )

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            num_entries_user_limit=user_input.search_settings.max_entries,
        )


class ForcesCreateExportWorkflowInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_id: str = Field(..., description='User ID performing the search.')
    output_file_format: list[DataFileFormatLiteral] = Field(
        ..., description='Output file format.'
    )
    properties: set[str] = Field(..., description='List of required fields.')
    max_frames: int | None = Field(
        ...,
        description='Maximum number of frames to export.',
    )


class ForcesExportDatasetMetadata(ExportDatasetMetadata):
    user_input: ForcesExportEntriesUserInput | None = Field(
        None, description='Original user input for the export entries workflow.'
    )  # type: ignore[assignment]
    num_frames_exported: int | None = Field(
        None, description='Number of frames exported in the dataset.'
    )  # type: ignore[assignment]


class ForcesWriteMetadataFileInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ...,
        description='ID of the export entries workflow.',
    )
    metadata: ForcesExportDatasetMetadata = Field(
        ..., description='Metadata to be written to the metadata file.'
    )


class ForcesOutputFile(OutputFile):
    num_frames_exported: int | None = Field(
        None, description='Number of frames exported in the dataset.'
    )  # type: ignore[assignment]


class ForcesExportOutputFile(BaseModel):
    manifest_output: PrepareManifestOutput = Field(
        ..., description='Output of the prepare manifest activity.'
    )
    output_file: ForcesOutputFile = Field(
        ..., description='Output of the create export workflow.'
    )


class ForcesManifestArchiveExportInput(BaseModel):
    manifest_data: PrepareManifestInput = Field(
        ..., description='Input data for the prepare manifest activity.'
    )
    export_data: ForcesCreateExportWorkflowInput = Field(
        ..., description='Input data for the create export workflow.'
    )
