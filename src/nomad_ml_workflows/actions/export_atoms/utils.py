import json
from collections.abc import Iterable
from pathlib import Path


def generate_atoms_from_archives(archives: Iterable[dict]) -> list:
    return [archive for archive in archives]


def write_atoms_to_file(atoms: list, output_file_path: str | Path):
    with open(output_file_path, 'w') as f:
        json.dump(atoms, f, indent=2)
    return len(atoms)
