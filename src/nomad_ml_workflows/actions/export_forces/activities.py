import json
from pathlib import Path

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.config import config as nomad_config
from nomad.utils import get_logger

try:
    from nomad_forces_export.config import REQUIRED_ARCHIVE_DATA
except ImportError:
    REQUIRED_ARCHIVE_DATA = {}
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    ManifestEntry,
    MetadataFile,
    OutputFile,
)
from nomad_ml_workflows.actions.export_entries.utils import generate_archives
from nomad_ml_workflows.actions.export_forces.models import (
    ForcesCreateExportWorkflowInput,
    ForcesWriteMetadataFileInput,
)
from nomad_ml_workflows.actions.export_forces.utils import (
    generate_atoms_from_archives,
    write_atoms_to_file,
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


@activity.defn(name=f'{ACTION_NAME}.read_archives_and_create_export')
def read_archives_and_create_export(
    data: ForcesCreateExportWorkflowInput,
) -> OutputFile:
    """
    Reads selected entry archives and writes the exported atoms dataset file (extxyz/ASE DB).
    """
    info = activity.info()
    activity_logger = logger.bind(activity_type=info.activity_type)

    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    file_extensions = [
        DATA_FILE_EXTENSIONS[format] for format in data.output_file_format
    ]
    data_subdirectory = artifacts_subdirectory
    if len(file_extensions) > 1:
        data_subdirectory = artifacts_subdirectory / DATA_ARTIFACT_NAME
        data_subdirectory.mkdir(parents=True, exist_ok=True)
    output_file_paths = [
        data_subdirectory / f'{DATA_ARTIFACT_NAME}.{ext}' for ext in file_extensions
    ]
    temporary_output_file_paths = [
        output_file_path.with_stem(f'{output_file_path.stem}.tmp')
        for output_file_path in output_file_paths
    ]
    # load manifest
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    archives = generate_archives(
        manifest, REQUIRED_ARCHIVE_DATA, data.user_id, activity_logger
    )

    atoms_generator = generate_atoms_from_archives(
        archives, properties=data.properties, max_frames=data.max_frames
    )
    write_atoms_to_file(
        atoms_generator,
        temporary_output_file_paths[0],
        output_format=data.output_file_format,
    )

    for i in range(len(temporary_output_file_paths)):
        if not temporary_output_file_paths[i].exists():
            logger.error(
                f'Expected output file {temporary_output_file_paths[i]} does not exist.'
            )
            continue
        temporary_output_file_paths[i].replace(output_file_paths[i])

    if len(output_file_paths) > 1:
        return OutputFile(
            file_path=data_subdirectory.as_posix(),
            file_size=sum(path.stat().st_size for path in output_file_paths),
            num_entries_exported=len(manifest),
        )

    return OutputFile(
        file_path=output_file_paths[0].as_posix(),
        file_size=output_file_paths[0].stat().st_size,
        num_entries_exported=len(manifest),
    )


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
