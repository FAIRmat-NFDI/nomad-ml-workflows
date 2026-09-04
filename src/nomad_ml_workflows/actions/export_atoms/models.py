import json
from typing import Literal

from nomad.app.v1.models.models import Query
from nomad.config import config as nomad_config
from pydantic import BaseModel, ConfigDict, Field

from nomad_ml_workflows.actions.export_entries.models import (
    ExportDatasetMetadata,
)

OwnerLiteral = Literal[
    'visible',
    'public',
    'user',
    'shared',
    'staging',
]
DataFileFormatLiteral = Literal['extxyz', 'asedb']


config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)


def _clean_field(field: str) -> str:
    """
    Removes trailing whitespaces and inverted commas
    """
    return field.strip().strip("'").strip('"')


_DIRECTIVE_PRIORITY = {'include': 1, 'include-resolved': 2, 'exclude': 3}


WORKFLOWS = [
    'SinglePoint',
    'single_point',
    'GeometryOptimization',
    'geometry_optimization',
]

BASE_QUERY = {
    'results.method.method_name:any': ['DFT'],
    'results.method.workflow_name:any': WORKFLOWS,
    'quantities:all': ['run.calculation', 'run.system'],
}


class IncludeProperties(BaseModel):
    Energies: bool = Field(False, description='Include energy data.')
    Forces: bool = Field(False, description='Include forces data.')
    Stresses: bool = Field(False, description='Include stress data.')


class AtomsSearchSettings(BaseModel):
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
    )

    @property
    def required_properties(self) -> list[str]:
        """
        Returns a list of required properties based on the user's selection.
        """
        required = []
        if self.properties.Energies:
            required.append('energy')
        if self.properties.Forces:
            required.append('forces')
        if self.properties.Stresses:
            required.append('stress')
        return required


class AtomsExportSettings(BaseModel):
    file_format: DataFileFormatLiteral = Field(
        'extxyz',
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


class AtomsExportEntriesUserInput(BaseModel):
    model_config = ConfigDict(title='')

    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )  # required field that is not shown in the Action Form UI
    upload_id: str = Field(
        ...,
        title='Destination project ID',
        description='ID of the project/upload where the exported artifacts will be saved.',
    )
    search_settings: AtomsSearchSettings = Field(..., title='Search options')
    export_settings: AtomsExportSettings = Field(..., title='Export options')


class AtomsExtractEntriesWorkflowInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_input: AtomsExportEntriesUserInput = Field(
        ..., description='Original user input for the export entries workflow.'
    )

class AtomsNormalizedSearchSettings(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    num_entries_user_limit: int = Field(
        ..., description='Maximum number of entries requested by the user.'
    )

    @classmethod
    def from_user_input(
        cls,
        user_input: AtomsExportEntriesUserInput,
    ) -> 'AtomsNormalizedSearchSettings':
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

class AtomsReadArchivesWorkflowInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_id: str = Field(..., description='User ID performing the search.')
    output_file_format: DataFileFormatLiteral = Field(
        ..., description='Output file format.'
    )
    properties: list[str] = Field(..., description='List of required fields.')


class AtomsExportDatasetMetadata(ExportDatasetMetadata):
    user_input: AtomsExportEntriesUserInput | None = Field(
        None, description='Original user input for the export entries workflow.'
    )  # type: ignore[assignment]


class AtomsWriteMetadataFileInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ...,
        description='ID of the export entries workflow.',
    )
    metadata: AtomsExportDatasetMetadata = Field(
        ..., description='Metadata to be written to the metadata file.'
    )
