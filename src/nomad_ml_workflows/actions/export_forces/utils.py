from __future__ import annotations

from collections.abc import Iterable, Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any

from nomad.actions.manager import action_instance_artifacts_dir

from nomad_ml_workflows.actions.export_entries.utils import (
    write_dicts_to_json,
)

try:
    from nomad_forces_export.config import REQUIRED_ARCHIVE_DATA, archive_filter
except ImportError:
    raise ImportError(
        'nomad-forces-export is required. Install with: '
        'pip install nomad-ml-workflows[cpu]'
    ) from e
from collections import defaultdict

from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, User
from nomad.archive.required import RequiredReader
from nomad.config import config as nomad_config
from nomad.files import UploadFiles
from nomad.search import search as nomad_search

from nomad_ml_workflows.actions.export_entries.models import (
    PrepareManifestInput,
)

MANIFEST_FILE_NAME = 'selected_entries'

config = nomad_config.get_plugin_entry_point('nomad_ml_workflows.actions:export_forces')


@lru_cache(maxsize=1)
def require_nomad_forces_export() -> tuple[Any, Any]:
    """Load optional nomad-forces-export dependency on demand."""
    try:
        from nomad_forces_export import atoms_generator, write_atoms
    except ImportError as e:
        raise ImportError(
            'nomad-forces-export is required. Install with: '
            'pip install nomad-ml-workflows[cpu]'
        ) from e
    return atoms_generator, write_atoms


def generate_atoms_from_archives(
    archives: Iterator, properties: list[str], max_frames: int | None = None
) -> Iterator:
    atoms_generator, _ = require_nomad_forces_export()
    return atoms_generator(archives, properties=set(properties), max_frames=max_frames)


def write_atoms_to_file(
    atoms: Iterator, output_file_path: str | Path, output_format: list[str] = ['extxyz']
) -> int:
    _, write_atoms = require_nomad_forces_export()
    return write_atoms(atoms, output_path=output_file_path, output_format=output_format)


def generate_archives_with_filter(archives: Iterator[dict]):
    """
    Generate archives that pass the filter defined in nomad_forces_export.config.archive_filter.
    """
    try:
        from nomad_forces_export.config import archive_filter
    except ImportError as e:
        raise ImportError(
            'nomad-forces-export is required. Install with: '
            'pip install nomad-ml-workflows[cpu]'
        ) from e

    for archive in archives:
        if not archive_filter(archive):
            yield archive


def yield_manifest_archives(
    data: PrepareManifestInput, response, logger=None
) -> Iterator[dict]:
    max_num_entries_limit = min(
        config.max_entries_export_limit,  # type: ignore
        data.num_entries_user_limit,
    )
    manifest: list = []
    page_size = min(10000, max_num_entries_limit)
    while True:
        required_reader = RequiredReader(
            required=REQUIRED_ARCHIVE_DATA,
            resolve_inplace=True,
            user=User(user_id=data.user_id),
        )

        # arrange entries by upload_id
        manifest_dict = defaultdict(list)
        for entry in response.data:
            manifest_dict[entry['upload_id']].append(entry['entry_id'])

        for upload_id, entry_ids in manifest_dict.items():
            if logger:
                logger.info(f'processing upload_id: {upload_id}')

            with UploadFiles.get(upload_id) as upload_files:
                if upload_files is None:
                    if logger:
                        logger.info(f'no upload files found for upload_id: {upload_id}')
                    continue

                for entry_id in entry_ids:
                    entry = {'entry_id': entry_id, 'upload_id': upload_id}
                    try:
                        with upload_files.read_archive(entry_id) as upload_archive:
                            entry['archive'] = required_reader.read(
                                upload_archive, entry_id, upload_id
                            )
                            if entry['archive'] is None or archive_filter(entry):
                                continue
                            manifest.append(
                                {'entry_id': entry_id, 'upload_id': upload_id}
                            )
                            yield entry
                            del entry
                            if len(manifest) >= max_num_entries_limit:
                                break
                    except Exception as e:
                        if logger:
                            logger.error(
                                'failed to read entry archive',
                                entry_id=entry_id,
                                upload_id=upload_id,
                                exc_info=e,
                            )
        del required_reader
        if len(manifest) >= max_num_entries_limit:
            break
        if response.pagination.next_page_after_value is None:
            # last page was already consumed
            break
        response = nomad_search(
            user_id=data.user_id,
            owner=data.owner,
            query=data.query,
            required=MetadataRequired(include=['entry_id', 'upload_id']),  # type: ignore
            pagination=MetadataPagination(
                page_size=page_size,
                page_after_value=response.pagination.next_page_after_value,
            ),  # type: ignore
        )
    manifest = manifest[:max_num_entries_limit]
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    write_dicts_to_json(manifest, manifest_file_path)
