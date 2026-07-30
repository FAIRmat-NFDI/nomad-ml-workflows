import json
from typing import Annotated, Any, Literal

from nomad.app.v1.models.models import MetadataPagination, Query, owner_documentation
from pydantic import BaseModel, Field

OwnerLiteral = Literal['public', 'visible', 'shared', 'user', 'staging']
BatchFileFormatLiteral = Literal['parquet', 'json']
OutputFileFormatLiteral = Literal['parquet', 'csv', 'json']


class Include(BaseModel):
    type: Literal['include'] = Field('include')
    path: str = Field(..., description='Archive paths to be included.')
    resolve_references: bool = Field(
        False,
        description='Recursively resolve references for the included path.',
    )


class Exclude(BaseModel):
    type: Literal['exclude'] = Field('exclude')
    path: str = Field(..., description='Archive paths to be excluded.')


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
        'visible', description='Owner of the entries to be searched.'
    )
    page_size: int = Field(
        1000,
        gt=0,
        description='Number of entries to be fetched and written per search page. '
        'Use smaller page sizes when exporting large entries to reduce memory usage.',
    )
    query: str = Field(
        ...,
        description="""Query for extracting entries. Should be a valid dictionary
        string. For example:
        {
            'entry_type': 'ELNSample'
        }""",
        json_schema_extra={
            'uiSchema': {'ui:widget': 'textarea', 'ui:options': {'rows': 5}}
        },
    )
    required: list[Required] | None = Field(
        None,
        description='Required archive paths for filtering the search results. '
        'Paths can target quantities like "results.method.method_name" or '
        'sub-sections like "results".',
        json_schema_extra={
            'uiSchema': {
                'items': {
                    'type': {'ui:widget': 'hidden'},
                },
            },
        },
    )


class OutputSettings(BaseModel):
    output_file_format: OutputFileFormatLiteral = Field(
        'parquet',
        description='Format of the output file.',
    )
    zip_output: bool = Field(
        True,
        description='Whether to create a zip file for the output file(s). Set it '
        'to true if you want download the dataset for external use. If you want to '
        'work with the exported data in NOMAD, set it to false. This will export the '
        'dataset as a directory in the specified project.',
    )


class ExportEntriesUserInput(BaseModel):
    upload_id: str = Field(
        ...,
        description='Unique identifier for the upload associated with the workflow.',
    )
    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )
    search_settings: SearchSettings
    output_settings: OutputSettings


class CreateArtifactSubdirectoryInput(BaseModel):
    subdir_name: str = Field(..., description='Name of the subdirectory to be created.')


class NormalizedSearchSettings(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    required: str | dict[str, Any] = Field(
        '*',
        description='Dictionary of required fields and directives compatible with '
        '`nomad.archive.required.RequiredReader` class.',
    )
    pagination: MetadataPagination = Field(
        ..., description='Pagination settings for the search results.'
    )

    @staticmethod
    def build_archive_required(required: list[Required] | None) -> str | dict[str, Any]:
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

        pagination = MetadataPagination(page_size=user_input.search_settings.page_size)  # type: ignore

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            required=archive_required,
            pagination=pagination,
        )


class SearchPageOutput(BaseModel):
    num_entries_exported: int = Field(
        ..., description='Number of entries exported to the output file.'
    )
    search_start_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the first search started.'
    )
    search_end_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the last search completed.'
    )


class ManifestEntry(BaseModel):
    entry_id: str = Field(..., description='Entry ID.')
    upload_id: str = Field(..., description='Upload ID.')


class PrepareManifestInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    pagination: MetadataPagination = Field(
        ..., description='Pagination settings for the search results.'
    )
    max_entries_export_limit: int = Field(
        ..., description='Maximum number of entries to be exported.'
    )
    manifest_file_path: str = Field(..., description='Path to the manifest file.')


class PrepapeManifestOutput(BaseModel):
    num_entries_available: int = Field(
        ..., description='Number of entries available for export.'
    )
    search_start_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the search started.'
    )
    search_end_time: str = Field(
        ..., description='UTC Timestamp (ISO) when the search ended.'
    )


class ReadArchivesWorkflowInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    output_file_format: str = Field(..., description='Output file format.')
    manifest_file_path: str = Field(..., description='Path to the manifest file.')
    artifact_subdirectory: str = Field(
        ..., description="Subdirectory where current workflow's artifacts are stored."
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


class CollectCursorsInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    page_size: int = Field(..., description='Number of entries per page.')
    max_entries_export_limit: int = Field(
        ..., description='Maximum number of entries to be exported.'
    )


class CollectCursorsOutput(BaseModel):
    page_after_values: list[str | None] = Field(
        ...,
        description='List of page_after_value cursors, one per page. '
        'The first entry is None (start of first page) when at least one page is '
        'available. If num_entries_available is 0, it is an empty list.',
    )
    num_entries_available: int = Field(
        ...,
        description='Total number of entries available for the given search query.',
    )
    num_pages: int = Field(
        ...,
        description='Total number of pages needed to export the entries, based on the '
        'page size and max entries export limit.',
    )


class RenameGeneratedFileInput(BaseModel):
    artifact_subdirectory: str = Field(
        ...,
        description='Subdirectory where the file will be renamed.',
    )
    output_file_format: OutputFileFormatLiteral = Field(
        ...,
        description='Format of the output file.',
    )
    generated_file_path: str = Field(
        ...,
        description='Path of generated file that will be renamed.',
    )


class MergeOutputFilesInput(BaseModel):
    artifact_subdirectory: str = Field(
        ...,
        description='Subdirectory where the merged output file will be stored.',
    )
    output_file_format: OutputFileFormatLiteral = Field(
        ...,
        description='Format of the output file.',
    )
    generated_file_paths: list[str] = Field(
        ...,
        description='List of the generated file paths to be merged into a single file.',
    )


class ExportDatasetMetadata(BaseModel):
    num_entries_exported: int = Field(
        0,
        description='Total number of entries exported in all the exported dataset '
        'batches.',
    )
    num_entries_available: int = Field(
        0,
        description='Total number of entries available for the given search query.',
    )
    reached_max_entries_limit: bool = Field(
        False,
        description='Indicates whether the export reached the maximum number of '
        'entries allowed. If true, the exported dataset contains the first N entries '
        'up to the maximum limit.',
    )
    search_start_time: str = Field(
        '',
        description='UTC Timestamp (ISO) when the first search batch started.',
    )
    search_end_time: str = Field(
        '',
        description='UTC Timestamp (ISO) when the last search batch completed.',
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
    user_id: str = Field(
        ..., description='User ID performing the export dataset operation.'
    )
    upload_id: str = Field(
        ..., description='Upload ID associated with the export dataset operation.'
    )
    artifact_subdirectory: str = Field(
        ...,
        description='Subdirectory where the exported dataset zip file will be stored.',
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
    source_paths: list[str] | None = Field(
        None, description='Paths to the source files of the dataset.'
    )
    metadata: ExportDatasetMetadata = Field(
        ..., description='Metadata associated with the exported dataset.'
    )


class CleanupArtifactsInput(BaseModel):
    subdir_path: str = Field(
        ..., description='Path to the subdirectory to be cleaned up.'
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
