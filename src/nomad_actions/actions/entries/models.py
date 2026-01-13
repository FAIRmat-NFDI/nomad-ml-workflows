import ast
from typing import Literal

from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, Query
from pydantic import BaseModel, Field

OwnerLiteral = Literal['public', 'visible', 'shared', 'user', 'staging']
OutputFileTypeLiteral = Literal['parquet', 'csv', 'json']
IndexLiteral = Literal['entries', 'datasets', 'models', 'spaces']


class SearchSettings(BaseModel):
    owner: OwnerLiteral = Field(
        'visible', description='Owner of the entries to be searched.'
    )
    query: str = Field(
        ...,
        description="""Query for extracting entries. Should be a valid dictionary
        string. For example:
        \'{
            "entry_type": "ELNSample"
        }\'""",
        # json_schema_extra={
        #     'ui:widget': 'textarea',  # Explicitly request textarea widget
        #     'ui:options': {
        #         'rows': 5  # Optional: control height
        #     },
        # },
    )
    required: str = Field(
        '',
        description="""Required fields for filtering the search results. Should be a
        valid dictionary with include and exclude lists. For example:
        \'{
            "include": ["results*", "data.results*"],
            "exclude": ["results.method.name"]
        }\'""",
    )


class OutputSettings(BaseModel):
    output_file_type: OutputFileTypeLiteral = Field(
        'parquet',
        description='Type of the output file to be generated.',
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


class SearchInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    required: MetadataRequired | None = Field(
        None, description='Required fields for filtering the search results.'
    )
    pagination: MetadataPagination = Field(
        ..., description='Pagination settings for the search results.'
    )
    output_file_path: str | None = Field(
        None, description='Path to the generated output file.'
    )

    @classmethod
    def from_user_input(
        cls,
        user_input: ExportEntriesUserInput,
        /,
        output_file_path: str,
    ) -> 'SearchInput':
        """Convert from ExportEntriesUserInput to SearchInput"""
        query = ast.literal_eval(user_input.search_settings.query)
        required = (
            MetadataRequired(**ast.literal_eval(user_input.search_settings.required))
            if user_input.search_settings.required
            else None
        )
        pagination = MetadataPagination()  # Use default pagination settings

        return cls(
            user_id=user_input.user_id,
            owner=user_input.search_settings.owner,
            query=query,
            required=required,
            pagination=pagination,
            output_file_path=output_file_path,
        )


class SearchOutput(BaseModel):
    pagination_next_page_after_value: str | None = Field(
        None,
        description='The next_page_after_value from pagination, if more results are '
        'available.',
    )


class ConsolidateOutputFilesInput(BaseModel):
    generated_file_paths: list[str] = Field(
        ..., description='List of the generated file paths to be consolidated.'
    )


class ExportDatasetInput(BaseModel):
    upload_id: str = Field(
        ...,
        description='Unique identifier for the upload where dataset will be saved.',
    )
    user_id: str = Field(
        ..., description='Unique identifier for the user saving the dataset.'
    )
    source_path: str = Field(..., description='Path to the source file of dataset.')


class CleanupArtifactsInput(BaseModel):
    subdir_path: str = Field(
        ..., description='Path to the subdirectory to be cleaned up.'
    )
