import json
from datetime import datetime, timezone
from pathlib import Path

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired
from nomad.config import config as nomad_config
from nomad.search import search as nomad_search
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    ManifestEntry,
    ManifestFile,
    MetadataFile,
    PrepareManifestOutput,
)
from nomad_ml_workflows.actions.export_forces.models import (
    ForcesExportOutputFile,
    ForcesManifestArchiveExportInput,
    ForcesOutputFile,
    ForcesWriteMetadataFileInput,
)
from nomad_ml_workflows.actions.export_forces.utils import (
    generate_atoms_from_archives,
    write_atoms_to_file,
    yield_manifest_archives,
)

config = nomad_config.get_plugin_entry_point('nomad_ml_workflows.actions:export_forces')
logger = get_logger(__name__)

DATA_ARTIFACT_NAME = 'data'
MANIFEST_FILE_NAME = 'selected_entries'
METADATA_FILE_NAME = 'metadata'

DATA_FILE_EXTENSIONS = {
    'extxyz': 'xyz',
    'ase_db': 'db',
}
ACTION_NAME = 'nomad_ml_workflows.actions:export_forces'


@activity.defn(name=f'{ACTION_NAME}.write_export_forces_metadata_file')
async def write_export_forces_metadata_file(
    data: ForcesWriteMetadataFileInput,
) -> MetadataFile:
    """Create a metadata.json file in the artifact subdirectory"""
    artifact_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    metadata_file_path = artifact_subdirectory / f'{METADATA_FILE_NAME}.json'
    metadata_dict = {
        'note': 'This metadata file contains information about the exported dataset '
        'and the conditions under which it was generated.',
        'data': data.metadata.model_dump(),
        'schema': data.metadata.model_json_schema(),
    }
    with open(metadata_file_path, 'w', encoding='utf-8') as metafile:
        json.dump(metadata_dict, metafile, indent=2)

    return MetadataFile(
        file_path=metadata_file_path.as_posix(),
        file_size=metadata_file_path.stat().st_size,
    )


@activity.defn(name=f'{ACTION_NAME}.manifest_archives_and_create_export')
def manifest_archives_and_create_export(
    data: ForcesManifestArchiveExportInput,
) -> ForcesExportOutputFile:
    """
    Reads selected entry archives and writes the exported atoms dataset file (extxyz/ASE DB).
    """
    info = activity.info()
    activity_logger = logger.bind(activity_type=info.activity_type)

    manifest_data = data.manifest_data
    export_data = data.export_data

    max_num_entries_limit = min(
        config.max_entries_export_limit,  # type: ignore
        manifest_data.num_entries_user_limit,
    )

    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(export_data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    file_extensions = [
        DATA_FILE_EXTENSIONS[format] for format in export_data.output_file_format
    ]
    data_subdirectory = artifacts_subdirectory
    if len(file_extensions) > 1:
        data_subdirectory = artifacts_subdirectory / DATA_ARTIFACT_NAME
        data_subdirectory.mkdir(parents=True, exist_ok=True)
    temp_file_stem = data_subdirectory / f'{DATA_ARTIFACT_NAME}.tmp'
    output_file_paths = [
        data_subdirectory / f'{DATA_ARTIFACT_NAME}.{ext}' for ext in file_extensions
    ]
    temporary_output_file_paths = [
        output_file_path.with_stem(f'{output_file_path.stem}.tmp')
        for output_file_path in output_file_paths
    ]

    page_size = min(10000, max_num_entries_limit)
    starttime = datetime.now(timezone.utc).isoformat()
    response = nomad_search(
        user_id=manifest_data.user_id,
        owner=manifest_data.owner,
        query=manifest_data.query,
        required=MetadataRequired(include=['entry_id', 'upload_id']),  # type: ignore
        pagination=MetadataPagination(page_size=page_size),  # type: ignore
    )
    num_entries_available = response.pagination.total

    # load manifest
    archives = yield_manifest_archives(manifest_data, response, activity_logger)
    atoms_generator = generate_atoms_from_archives(
        archives,
        properties=export_data.properties,
        max_frames=export_data.max_frames,
    )
    num_of_frames = write_atoms_to_file(
        atoms_generator,
        temp_file_stem,
        output_format=export_data.output_file_format,
    )

    for i in range(len(temporary_output_file_paths)):
        if not temporary_output_file_paths[i].exists():
            logger.error(
                f'Expected output file {temporary_output_file_paths[i]} does not exist.'
            )
            continue
        temporary_output_file_paths[i].replace(output_file_paths[i])
    endtime = datetime.now(timezone.utc).isoformat()
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]
    reached_max_entries_limit = num_entries_available > max_num_entries_limit
    manifest = manifest[:max_num_entries_limit]
    num_entries_selected = len(manifest)
    activity_logger.info(
        f'Exported {num_of_frames} frames from {num_entries_selected} entries'
    )
    manifest_file = ManifestFile(
        file_path=manifest_file_path.as_posix(),
        file_size=manifest_file_path.stat().st_size,
    )
    manifest_output = PrepareManifestOutput(
        search_start_time=starttime,
        search_end_time=endtime,
        num_entries_available=num_entries_available,
        num_entries_selected=num_entries_selected,
        reached_max_entries_limit=reached_max_entries_limit,
        manifest_file=manifest_file,
    )
    output_file = ForcesOutputFile(
        file_path=(
            data_subdirectory.as_posix()
            if len(output_file_paths) > 1
            else output_file_paths[0].as_posix()
        ),
        file_size=sum(path.stat().st_size for path in output_file_paths),
        num_entries_exported=len(manifest),
        num_frames_exported=num_of_frames,
    )

    return ForcesExportOutputFile(
        manifest_output=manifest_output,
        output_file=output_file,
    )
