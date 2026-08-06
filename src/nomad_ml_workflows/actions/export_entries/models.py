import json
from typing import Annotated, Any, Literal

from nomad.app.v1.models.models import Query
from nomad.config import config as nomad_config
from pydantic import BaseModel, ConfigDict, Field

OwnerLiteral = Literal[
    'visible',
    'public',
    'user',
    'shared',
    'staging',
]
DataFileFormatLiteral = Literal['parquet', 'csv', 'json']

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)


class Include(BaseModel):
    type: Literal['include'] = Field('include')
    path: str = Field(
        ...,
        title='Archive path',
        description='Dot-separated path to an archive quantity or section for inclusion.',
    )
    resolve_references: bool = Field(
        False,
        title='Resolve references',
        description='Include data reached through references below this path.',
    )


class Exclude(BaseModel):
    type: Literal['exclude'] = Field('exclude')
    path: str = Field(
        ...,
        title='Archive path',
        description='Dot-separated path to an archive quantity or section for exclusion.',
    )


Required = Annotated[Include, Field(discriminator='type')]
# TODO: set Required = Annotated[Include | Exclude, Field(discriminator='type')]
# once exclude directive is supported in RequiredReader


def _clean_field(field: str) -> str:
    """
    Removes trailing whitespaces and inverted commas
    """
    return field.strip().strip("'").strip('"')


_DIRECTIVE_PRIORITY = {'include': 1, 'include-resolved': 2, 'exclude': 3}


def _path_and_directive(item: Include | Exclude) -> tuple[tuple[str, ...], str]:
    """Normalize one user requirement."""
    parts = tuple(
        part for part in _clean_field(item.path).rstrip('*').split('.') if part
    )
    if isinstance(item, Include):
        directive = 'include-resolved' if item.resolve_references else 'include'
    elif isinstance(item, Exclude):
        directive = 'exclude'
    else:
        raise TypeError(f'Unsupported requirement type: {type(item).__name__}')
    return parts, directive


def _add_required_path(
    result: dict[str, Any], parts: tuple[str, ...], directive: str
) -> None:
    """Add one path, keeping the strongest directive at each level."""
    node = result
    for part in parts[:-1]:
        current = node.get(part)
        if isinstance(current, str):
            if _DIRECTIVE_PRIORITY[current] >= _DIRECTIVE_PRIORITY[directive]:
                return
            current = {'*': current}
            node[part] = current
        node = node.setdefault(part, {})

    leaf = parts[-1]
    current = node.get(leaf)
    if isinstance(current, dict):
        current['*'] = max(
            current.get('*', directive),
            directive,
            key=_DIRECTIVE_PRIORITY.__getitem__,
        )
    elif (
        current is None or _DIRECTIVE_PRIORITY[directive] > _DIRECTIVE_PRIORITY[current]
    ):
        node[leaf] = directive


def _contains_include(required: str | dict[str, Any]) -> bool:
    """Return whether the final specification contains an include directive."""
    if isinstance(required, str):
        return required in {'include', 'include-resolved'}
    return any(_contains_include(value) for value in required.values())


class SearchSettings(BaseModel):
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
        ...,
        title='Search query',
        description='NOMAD search query written as a JSON object.',
        json_schema_extra={
            'uiSchema': {
                'ui:widget': 'textarea',
                'ui:placeholder': '{\n  "entry_type": "ELNSample"\n}',
                'ui:help': (
                    'You can also copy the query from the **View API Call** '
                    'dialog in a NOMAD search app.'
                ),
                'ui:options': {'rows': 5, 'enableMarkdownInHelp': True},
            }
        },
    )
    required: list[Required] = Field(
        default_factory=list,
        title='Archive required paths',
        description=(
            'Customize the exported archive content using quantity or section paths. '
            'Leave this empty to export the complete archive.'
        ),
        json_schema_extra={
            'uiSchema': {
                'items': {
                    'type': {'ui:widget': 'hidden'},
                    'path': {'ui:placeholder': 'results.method.method_name'},
                    'resolve_references': {
                        'ui:help': 'Include data reached through references below this path.',
                    },
                },
            },
        },
    )


class ExportSettings(BaseModel):
    file_format: DataFileFormatLiteral = Field(
        'parquet',
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


class ExportEntriesUserInput(BaseModel):
    model_config = ConfigDict(title='')

    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )  # required field that is not shown in the Action Form UI
    upload_id: str = Field(
        ...,
        title='Destination project ID',
        description='ID of the project/upload where the exported artifacts will be saved.',
    )
    search_settings: SearchSettings = Field(..., title='Search options')
    export_settings: ExportSettings = Field(..., title='Export options')


class CreateArtifactSubdirectoryInput(BaseModel):
    subdir_name: str = Field(..., description='Name of the subdirectory to be created.')


class NormalizedSearchSettings(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    num_entries_user_limit: int = Field(
        ..., description='Maximum number of entries requested by the user.'
    )
    required: str | dict[str, Any] = Field(
        '*',
        description='Dictionary of required fields and directives compatible with '
        '`nomad.archive.required.RequiredReader` class.',
    )

    @staticmethod
    def build_archive_required(required: list[Required]) -> str | dict[str, Any]:
        """Convert archive paths to a RequiredReader specification."""
        if not required:
            return '*'

        paths = [_path_and_directive(item) for item in required]
        paths = [(parts, directive) for parts, directive in paths if parts]
        if not paths:
            return '*'

        # With only exclusions, start from the complete archive.
        archive_required: dict[str, Any] = (
            {}
            if any(isinstance(item, Include) for item in required)
            else {'*': 'include'}
        )

        # Insert parents first so they absorb redundant child requirements.
        for parts, directive in sorted(paths, key=lambda item: len(item[0])):
            _add_required_path(archive_required, parts, directive)

        # Exclusions need a complete archive from which fields can be removed.
        if not _contains_include(archive_required):
            archive_required = {'*': 'include', **archive_required}

        # TODO: Remove this block once RequiredReader supports "*" keys.
        # For now, a resolved child promotes its included parent.
        included_paths = [parts for parts, value in paths if value == 'include']
        resolved_paths = [
            parts for parts, value in paths if value == 'include-resolved'
        ]
        for resolved_path in resolved_paths:
            parent_path = min(
                (
                    path
                    for path in included_paths
                    if resolved_path[: len(path)] == path
                    and len(resolved_path) > len(path)
                ),
                key=len,
                default=None,
            )
            if parent_path is None:
                continue
            node = archive_required
            for part in parent_path[:-1]:
                node = node[part]
            if isinstance(node.get(parent_path[-1]), dict):
                node[parent_path[-1]] = 'include-resolved'

        return archive_required

    @classmethod
    def from_user_input(
        cls,
        user_input: ExportEntriesUserInput,
    ) -> 'NormalizedSearchSettings':
        query = json.loads(
            _clean_field(user_input.search_settings.query).replace("'", '"')
        )

        archive_required = cls.build_archive_required(
            user_input.search_settings.required
        )

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            num_entries_user_limit=user_input.search_settings.max_entries,
            required=archive_required,
        )


class ManifestEntry(BaseModel):
    entry_id: str = Field(..., description='Entry ID.')
    upload_id: str = Field(..., description='Upload ID.')


class PrepareManifestInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID for the export entries workflow.'
    )
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    num_entries_user_limit: int = Field(
        ..., description='Number of entries requested by the user.'
    )


class ManifestFile(BaseModel):
    file_path: str = Field(..., description='Path to the manifest file.')
    file_size: int = Field(..., description='Size of the manifest file in bytes.')


class PrepareManifestOutput(BaseModel):
    num_entries_available: int = Field(
        ..., description='Total number of entries matching the search query.'
    )
    num_entries_selected: int = Field(
        ...,
        description='Number of matching entries selected for export after applying '
        'the export limit.',
    )
    search_start_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the search started.'
    )
    search_end_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the search ended.'
    )
    reached_max_entries_limit: bool = Field(
        ...,
        description='Whether the number of matching entries exceeded the export limit.',
    )
    manifest_file: ManifestFile = Field(
        ..., description='Manifest file containing the list of entries to export.'
    )


class ReadArchivesWorkflowInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_id: str = Field(..., description='User ID performing the search.')
    output_file_format: DataFileFormatLiteral = Field(
        ..., description='Output file format.'
    )
    required: str | dict[str, Any] = Field(
        '*',
        description='Dictionary of required fields and directives compatible with '
        '`nomad.archive.required.RequiredReader` class.',
    )


class OutputFile(BaseModel):
    file_path: str = Field(..., description='Path to the output file.')
    file_size: int = Field(..., description='Size of the output file in bytes.')
    num_entries_exported: int = Field(..., description='Number of entries exported.')


class ExportDatasetMetadata(BaseModel):
    num_entries_exported: int = Field(
        0,
        description='Number of entries that were successfully exported in the data file. ',
    )
    num_entries_available: int = Field(
        0,
        description='Total number of entries matching the search query.',
    )
    num_entries_selected: int = Field(
        0,
        description='Number of matching entries selected for export after applying '
        'the export limit.',
    )
    reached_max_entries_limit: bool = Field(
        False,
        description='Indicates whether the number of matching entries exceeded the '
        'export limit. If true, the exported dataset contains only the first N entries '
        'up to that limit.',
    )
    search_start_time: str = Field(
        '',
        description='UTC Timestamp (ISO) when the search for entries started.',
    )
    search_end_time: str = Field(
        '',
        description='UTC Timestamp (ISO) when the search for entries ended.',
    )
    nomad_deployment_api_host: str = Field(
        '',
        description=('API host of the NOMAD deployment that ran this workflow.'),
    )
    nomad_version: str = Field(
        '',
        description='Version of nomad-lab package available on the server that ran this workflow.',
    )
    nomad_ml_workflows_version: str = Field(
        '',
        description='Version of nomad-ml-workflows package available on the server that ran this workflow.',
    )
    user_input: ExportEntriesUserInput | None = Field(
        None, description='Original user input for the export entries workflow.'
    )
    error_info: str | None = Field(
        None,
        description='Error information if any error occurred during the search and '
        'merging process.',
    )


class ExportDatasetInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    user_id: str = Field(
        ..., description='User ID performing the export dataset operation.'
    )
    upload_id: str = Field(
        ..., description='Upload ID associated with the export dataset operation.'
    )
    zip_output: bool = Field(
        ...,
        description='Whether to create a zip file for the exported dataset.',
    )
    exportable_dir_name: str = Field(
        ...,
        description='Name of the directory containing the dataset that will be '
        'exported.',
    )
    source_paths: list[str] = Field(
        ..., description='Paths to the source files of the dataset.'
    )
    metadata: ExportDatasetMetadata = Field(
        ..., description='Metadata associated with the exported dataset.'
    )


class CleanupArtifactsInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )


class ExportEntriesOutput(BaseModel):
    exported_dir_path: str = Field(
        ...,
        description='Relative path, within the upload raw directory, of the '
        'directory containing the exported dataset.',
    )
    workflow_duration: float = Field(
        ...,
        description='Total duration of the Export Entries workflow in seconds, '
        'including any idle time.',
    )
