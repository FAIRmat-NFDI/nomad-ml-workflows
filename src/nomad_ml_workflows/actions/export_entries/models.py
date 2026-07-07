import json
from typing import Any, Literal

from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, Query
from pydantic import BaseModel, Field

OwnerLiteral = Literal['public', 'visible', 'shared', 'user', 'staging']
BatchFileTypeLiteral = Literal['parquet', 'json']
OutputFileTypeLiteral = Literal['parquet', 'csv', 'json']


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
    read_archives: bool = Field(
        False,
        description='Read full archive data, including non-indexed fields such as '
        'n-dim arrays. Disable to export only indexed fields faster.',
    )
    required_include: list[str] = Field(
        [],
        description='List of fields to include in the search results. For example: '
        'results*, data.results*',
    )
    required_include_resolved: list[str] = Field(
        [],
        description='Paths to include, along with resolved referencs, in the exported '
        'dataset. For example: results*, data.results*',
    )
    required_exclude: list[str] = Field(
        [],
        description='List of fields to exclude from the search results. For example: '
        'results.method.method_name',
    )


class OutputSettings(BaseModel):
    output_file_type: OutputFileTypeLiteral = Field(
        'parquet',
        description='Type of the output file.',
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


class Required(MetadataRequired):
    include_resolved: list[str] | None = Field(
        None,
        description='Quantities to include for each result. If the quantities include '
        'references to other sections, the references will be resolved.'
        'For example: results*, data.results*',
    )


class SearchPageInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    read_archives: bool = Field(
        ...,
        description='Read full archive data, including non-indexed fields such as '
        'n-dim arrays.',
    )
    required: Required = Field(
        ..., description='Required fields for filtering the search results.'
    )
    pagination: MetadataPagination = Field(
        ..., description='Pagination settings for the search results.'
    )
    batch_file_type: BatchFileTypeLiteral = Field(
        ..., description='Type of the output file.'
    )
    output_file_path: str = Field(..., description='Path to the generated output file.')
    max_entries_export_limit: int = Field(
        ..., description='Maximum number of entries to be exported.'
    )

    @classmethod
    def from_user_input(
        cls,
        user_input: ExportEntriesUserInput,
        /,
        output_file_path: str,
        max_entries_export_limit: int,
    ) -> 'SearchPageInput':
        """Convert from ExportEntriesUserInput to SearchPageInput"""

        def _clean_field(field: str) -> str:
            """
            Removes trailing whitespaces and inverted commas
            """
            return field.strip().strip("'").strip('"')

        query = json.loads(
            _clean_field(user_input.search_settings.query).replace("'", '"')
        )

        required = Required()
        if user_input.search_settings.required_include:
            include = [
                _clean_field(field)
                for field in user_input.search_settings.required_include
            ]
            required.include = include if include else None
        if user_input.search_settings.required_include_resolved:
            include_resolved = [
                _clean_field(field)
                for field in user_input.search_settings.required_include_resolved
            ]
            required.include_resolved = include_resolved if include_resolved else None
        if user_input.search_settings.required_exclude:
            exclude = [
                _clean_field(field)
                for field in user_input.search_settings.required_exclude
            ]
            required.exclude = exclude if exclude else None

        pagination = MetadataPagination(page_size=user_input.search_settings.page_size)

        batch_file_type = user_input.output_settings.output_file_type
        if batch_file_type == 'csv':
            batch_file_type = 'parquet'  # use parquet batches for csv

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            read_archives=user_input.search_settings.read_archives,
            required=required,
            pagination=pagination,
            batch_file_type=batch_file_type,
            output_file_path=output_file_path,
            max_entries_export_limit=max_entries_export_limit,
        )


class ReadArchivesInput(SearchPageInput):
    required: str | dict[str, Any] = Field(
        '*',
        description='Dictionary of required fields and directives compatible with '
        '`nomad.archive.required.RequiredReader` class.',
    )

    @staticmethod
    def build_archive_required(required: Required) -> str | dict:
        """
        Convert dot-separated required paths into a nested-dict structure with
        directives on the leaf. Output can be used when instantiating the
        `nomad.archive.required.RequiredReader` class.

        Adds the following directives at the dict nodes:
            - "include"             (for `required.include` list)
            - "include-resolved"    (for `required.include_resolved` list)

        Ignores the `required.exclude` list, since there is no support for
        "exclude" directives.

        If the required lists are empty, returns "*", meaning to include everything.

        Examples:
            - When required.include is:
                []                         -> "*"
                ["data"]                   -> {"data": "include"}
                ["data.num_val"]           -> {"data": {"num_val": "include"}}
                ["data.sub_sec"]           -> {"data": {"sub_sec": "include"}}
                ["data.sub_sec*"]          -> {"data": {"sub_sec": "include"}}
                                            (remove trailing *)
                ["data", "data.sub_sec"]   -> {"data": "*"}
                                            (parent wins)

        For `required.include_resolved`, the directive changes to "include-resolved",
        but the conversion follows the same logic.

        When same fields are present in `required.include` and
        `required.include_resolved`, "include-resolved" directive takes priority.
        """
        include = required.include or []
        include_resolved = required.include_resolved or []

        if not include and not include_resolved:
            return '*'  # include everything

        def _remove_wildcard(require_list: list) -> list:
            return [el[:-1] if el.endswith('*') else el for el in require_list]

        def _path_parts(path: str) -> list[str]:
            return [part for part in path.split('.') if part]

        def _add_include(
            target: dict[str, Any], path: str, directive='include'
        ) -> None:
            node = target
            parts = _path_parts(path)
            for index, part in enumerate(parts):
                is_leaf = index == len(parts) - 1
                if is_leaf:
                    node[part] = directive
                    return
                child = node.get(part)
                if child == directive:
                    return
                if not isinstance(child, dict):
                    child = {}
                    node[part] = child
                node = child

        include = _remove_wildcard(include)
        include_resolved = _remove_wildcard(include_resolved)

        archive_required = {}
        for path in include:
            _add_include(archive_required, path, directive='include')
        for path in include_resolved:
            _add_include(archive_required, path, directive='include-resolved')

        return archive_required

    @classmethod
    def from_search_page_input(
        cls,
        spi: SearchPageInput,
    ) -> 'ReadArchivesInput':
        """Convert from SearchPageInput to ReadArchivesInput"""

        required = ReadArchivesInput.build_archive_required(spi.required)

        return cls(
            user_id=spi.user_id,
            owner=spi.owner,
            query=spi.query,
            read_archives=spi.read_archives,
            required=required,
            pagination=spi.pagination,
            batch_file_type=spi.batch_file_type,
            output_file_path=spi.output_file_path,
            max_entries_export_limit=spi.max_entries_export_limit,
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
    output_file_type: OutputFileTypeLiteral = Field(
        ...,
        description='Type of the output file.',
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
