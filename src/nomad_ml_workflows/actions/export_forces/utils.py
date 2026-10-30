from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, User
from nomad.archive.required import RequiredReader
from nomad.config import config as nomad_config
from nomad.files import UploadFiles
from nomad.search import search as nomad_search

from nomad_ml_workflows.actions.export_entries.models import (
    PrepareManifestInput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    write_dicts_to_json,
)
from nomad_ml_workflows.actions.export_forces.models import build_metadata_required

MANIFEST_FILE_NAME = 'selected_entries'

config = nomad_config.get_plugin_entry_point('nomad_ml_workflows.actions:export_forces')


@lru_cache(maxsize=1)
def require_nomad_forces_export() -> tuple[Any, Any]:
    """Load optional nomad-forces-export dependency on demand."""
    try:
        from nomad_forces_export import (
            atoms_generator,
            write_atoms,
        )
    except ImportError as e:
        raise ImportError(
            'nomad-forces-export is required. Install with: '
            'pip install nomad-ml-workflows[cpu]'
        ) from e
    return atoms_generator, write_atoms


@lru_cache(maxsize=1)
def require_nomad_force_export_config():
    try:
        from nomad_forces_export.config import REQUIRED_ARCHIVE_DATA, archive_filter
    except ImportError as e:
        raise ImportError(
            'nomad-forces-export is required. Install with: '
            'pip install nomad-ml-workflows[cpu]'
        ) from e
    return REQUIRED_ARCHIVE_DATA, archive_filter


def merge_required(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge two RequiredReader specs without mutating inputs; '*' wins."""
    merged = dict(base)
    for key, value in extra.items():
        current = merged.get(key)
        if current is None:
            merged[key] = value
        elif isinstance(current, dict) and isinstance(value, dict):
            merged[key] = merge_required(current, value)
        elif isinstance(current, dict):
            merged[key] = value if value == '*' else current
        elif current != '*' and isinstance(value, str):
            merged[key] = value
    return merged


_INDEX_PATTERN = re.compile(
    r'^(?P<name>[^\[\]]+)\[(?P<start>-?\d*)(?P<colon>:?)(?P<stop>-?\d*)\]$'
)


def extract_metadata_values(
    archive: dict[str, Any], required: dict[str, Any], _prefix: str = ''
) -> dict[str, Any]:
    """Map each dot-separated path in a required spec to its archive value.

    Keys may carry a list index or slice, e.g. `ions[0]` or `ions[0:2]`.
    Paths missing from the archive are omitted.
    """
    values: dict[str, Any] = {}
    for key, spec in required.items():
        match = _INDEX_PATTERN.match(key)
        name = match['name'] if match else key
        if not isinstance(archive, dict) or name not in archive:
            continue
        value = archive[name]
        if match:
            if not isinstance(value, list):
                continue
            if match['colon']:
                value = value[
                    int(match['start']) if match['start'] else None : int(match['stop'])
                    if match['stop']
                    else None
                ]
            elif match['start']:
                try:
                    value = value[int(match['start'])]
                except IndexError:
                    continue
            else:
                continue
        path = f'{_prefix}{key}'
        if isinstance(spec, dict) and isinstance(value, dict):
            values.update(extract_metadata_values(value, spec, f'{path}.'))
        elif isinstance(spec, dict) and isinstance(value, list) and match:
            values[path] = [
                extract_metadata_values(item, spec) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            values[path] = value
    return values


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


def yield_manifest_archives(  # noqa: PLR0912
    data: PrepareManifestInput, response, required_metadata=[], logger=None
) -> Iterator[dict]:
    REQUIRED_ARCHIVE_DATA, archive_filter = require_nomad_force_export_config()
    required_metadata_paths = {
        m.path: m.name if m.name else m.path for m in required_metadata if m.path
    }
    resolved_metadata_required = build_metadata_required(
        list(required_metadata_paths.keys())
    )
    merged_required = merge_required(REQUIRED_ARCHIVE_DATA, resolved_metadata_required)
    max_num_entries_limit = min(
        config.max_entries_export_limit,  # type: ignore
        data.num_entries_user_limit,
    )
    manifest: list = []
    page_size = min(10000, max_num_entries_limit)
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'

    def write_manifest_file(manifest: list, manifest_file_path: Path):
        manifest = manifest[:max_num_entries_limit]
        write_dicts_to_json(manifest, manifest_file_path)

    while True:
        required_reader = RequiredReader(
            # required=REQUIRED_ARCHIVE_DATA,
            required=merged_required,
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
            upload_files = UploadFiles.get(upload_id)
            if upload_files is None:
                if logger:
                    logger.info(f'no upload files found for upload_id: {upload_id}')
                continue
            with upload_files:
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
                            write_manifest_file(manifest, manifest_file_path)
                            extra_metadata = extract_metadata_values(
                                entry['archive'], resolved_metadata_required
                            )
                            extra_metadata = {
                                required_metadata_paths.get(path, path): value
                                for path, value in extra_metadata.items()
                            }
                            if extra_metadata:
                                entry['archive']['extra_metadata'] = extra_metadata
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
    write_manifest_file(manifest, manifest_file_path)
