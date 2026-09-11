from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any


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
    archives: Iterable[dict], properties: list[str], max_frames: int | None = None
) -> Iterable:
    atoms_generator, _ = require_nomad_forces_export()
    return atoms_generator(archives, properties=set(properties), max_frames=max_frames)


def write_atoms_to_file(
    atoms: Iterable, output_file_path: str | Path, output_format: list[str] = ['extxyz']
) -> None:
    _, write_atoms = require_nomad_forces_export()
    write_atoms(atoms, output_path=output_file_path, output_format=output_format)
