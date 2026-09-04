import json
from pathlib import Path

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.config import config as nomad_config
from nomad.utils import get_logger
from nomad_mlip_data.config import REQUIRED_ARCHIVE_DATA
from temporalio import activity

from nomad_ml_workflows.actions.export_atoms.models import (
    AtomsReadArchivesWorkflowInput,
    AtomsWriteMetadataFileInput,
)
from nomad_ml_workflows.actions.export_atoms.utils import (
    generate_atoms_from_archives,
    write_atoms_to_file,
)
from nomad_ml_workflows.actions.export_entries.models import (
    ManifestEntry,
    MetadataFile,
    OutputFile,
)
from nomad_ml_workflows.actions.export_entries.utils import generate_archives

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)
logger = get_logger(__name__)

DATA_ARTIFACT_NAME = 'data'
MANIFEST_FILE_NAME = 'selected_entries'
METADATA_FILE_NAME = 'metadata'

DATA_FILE_EXTENSIONS = {
    'extxyz': 'xyz',
    'ase_db': 'db',
}


@activity.defn
def read_archives_and_generate_atoms(
    data: AtomsReadArchivesWorkflowInput,
) -> OutputFile:
    """
    Reads the archives and writes the output JSON file.
    """
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    file_extension = DATA_FILE_EXTENSIONS[data.output_file_format]
    output_file_path = artifacts_subdirectory / f'{DATA_ARTIFACT_NAME}.{file_extension}'
    temporary_output_file_path = output_file_path.with_stem(
        f'{output_file_path.stem}.tmp'
    )
    # load manifest
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    info = activity.info()
    activity_logger = logger.bind(activity_type=info.activity_type)

    archives = generate_archives(
        manifest, REQUIRED_ARCHIVE_DATA, data.user_id, activity_logger
    )

    atoms = generate_atoms_from_archives(archives, properties=data.properties)
    write_atoms_to_file(
        atoms, temporary_output_file_path, output_format=data.output_file_format
    )
    temporary_output_file_path.replace(output_file_path)
    return OutputFile(
        file_path=output_file_path.as_posix(),
        file_size=output_file_path.stat().st_size,
        num_entries_exported=len(atoms),
    )


@activity.defn
async def atoms_write_metadata_file(data: AtomsWriteMetadataFileInput) -> MetadataFile:
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
