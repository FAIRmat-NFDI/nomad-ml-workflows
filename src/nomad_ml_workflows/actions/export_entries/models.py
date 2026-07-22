import json
from typing import Any, Literal

from nomad.app.v1.models.models import MetadataPagination, Query
from pydantic import BaseModel, Field

OwnerLiteral = Literal['public', 'visible', 'shared', 'user', 'staging']
BatchFileFormatLiteral = Literal['parquet', 'json']
OutputFileFormatLiteral = Literal['parquet', 'csv', 'json']


def _clean_field(field: str) -> str:
    """
    Removes trailing whitespaces and inverted commas
    """
    return field.strip().strip("'").strip('"')


class Include(BaseModel):
    path: str = Field(..., description='Archive paths to be included.')
    resolve_references: bool = Field(
        False,
        description='Recursively resolve references for the included path.',
    )


class Exclude(BaseModel):
    path: str = Field(..., description='Archive paths to be excluded.')


Required = Include
# TODO: set "Required = Include | Exclude" once exclude directive is
# supported in RequiredReader


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


class SearchPageInput(BaseModel):
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
    batch_file_format: BatchFileFormatLiteral = Field(
        ..., description='Format of the output file.'
    )
    output_file_path: str = Field(..., description='Path to the generated output file.')
    max_entries_export_limit: int = Field(
        ..., description='Maximum number of entries to be exported.'
    )
    page_num: int = Field(..., description='Page number for the search results.')

    @staticmethod
    def build_archive_required(required: list[Required] | None) -> str | dict[str, Any]:
        """Convert archive path requirements to a RequiredReader specification.

        Includes select an archive path, optionally resolving all references below
        it. Excludes remove a path from an included subtree. When only exclusions
        are supplied, the rest of the archive is included by default.
        """
        if not required:
            return '*'

        directive_priority = {
            'include': 1,
            'include-resolved': 2,
            'exclude': 3,
        }  # 3 is highest priority
        directives: dict[tuple[str, ...], str] = {}
        has_include = False

        for r in required:
            path = _clean_field(r.path).rstrip('*')
            path_parts = tuple(part for part in path.split('.') if part)
            if not path_parts:
                continue

            if isinstance(r, Include):
                has_include = True
                directive = 'include-resolved' if r.resolve_references else 'include'
            elif isinstance(r, Exclude):
                directive = 'exclude'
            else:
                raise AssertionError(
                    f'Only instances of Include or Exclude allowed, got "{r: type(r)}"'
                )

            directives[path_parts] = max(
                directives.get(path_parts, directive),
                directive,
                key=directive_priority.__getitem__,
            )  # if same path_parts has multiple directives, pick the higher priority one

        if not directives:
            return '*'

        directive_key = object()
        tree: dict[Any, Any] = {}
        if not has_include:
            tree[directive_key] = 'include'

        for path_parts, directive in sorted(
            directives.items(), key=lambda item: len(item[0])
        ):
            inherited_directive = next(
                (
                    directives[path_parts[:index]]
                    for index in range(len(path_parts) - 1, 0, -1)
                    if path_parts[:index] in directives
                ),
                None,
            )
            if directive == inherited_directive:
                continue

            node = tree
            for part in path_parts:
                node = node.setdefault(part, {})
            node[directive_key] = directive

        def _render(node: dict[Any, Any]) -> str | dict[str, Any]:
            directive = node.get(directive_key)
            children = {
                key: _render(value)
                for key, value in node.items()
                if key is not directive_key
            }
            if directive is None:
                return children
            if not children:
                return directive
            return {'*': directive, **children}

        return _render(tree)

    @classmethod
    def from_user_input(
        cls,
        user_input: ExportEntriesUserInput,
        /,
        page_num: int,
        output_file_path: str,
        max_entries_export_limit: int,
    ) -> 'SearchPageInput':
        """Convert from ExportEntriesUserInput to SearchPageInput"""

        query = json.loads(
            _clean_field(user_input.search_settings.query).replace("'", '"')
        )

        archive_required = cls.build_archive_required(
            user_input.search_settings.required
        )

        pagination = MetadataPagination(page_size=user_input.search_settings.page_size)

        batch_file_format = user_input.output_settings.output_file_format
        if batch_file_format == 'csv':
            batch_file_format = 'parquet'  # use parquet batches for csv

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            required=archive_required,
            pagination=pagination,
            batch_file_format=batch_file_format,
            page_num=page_num,
            output_file_path=output_file_path,
            max_entries_export_limit=max_entries_export_limit,
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
    source_paths: list[str] = Field(
        ..., description='List of paths to the source files of the dataset.'
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
