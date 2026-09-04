from collections.abc import Iterable
from pathlib import Path

from nomad_mlip_data import atoms_generator, write_atoms


def generate_atoms_from_archives(
    archives: Iterable[dict], properties: list[str]
) -> list:
    return [atom for atom in atoms_generator(archives, properties=set(properties))]


def write_atoms_to_file(
    atoms: list, output_file_path: str | Path, output_format: str = 'extxyz'
) -> None:
    write_atoms(atoms, output_path=output_file_path, output_format=output_format)
