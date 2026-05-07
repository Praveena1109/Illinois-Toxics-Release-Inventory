"""
SCRIPT 06: FIXITY MANIFEST GENERATOR
Illinois TRI Dataset Curation | 2010–2024

PURPOSE:
Generate file-level checksums for repository preservation and archival packaging.

This script creates:
    docs/fixity_manifest.csv
    docs/fixity_manifest.json

The manifest records:
    - relative file path
    - file size in bytes
    - MD5 checksum
    - SHA-256 checksum
    - timestamp generated

WHY THIS MATTERS:
Fixity information helps verify that files have not changed unexpectedly after
curation, deposit, or transfer. This supports preservation, reproducibility, and
repository trust.
"""

from pathlib import Path
import hashlib
import csv
import json
from datetime import datetime, timezone

ROOT = Path(".")
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(exist_ok=True)

INCLUDE_DIRS = ["outputs", "docs", "metadata", "scripts"]
INCLUDE_ROOT_FILES = ["README.md", "requirements.txt", "environment.yml", "CITATION.cff", "LICENSE"]

EXCLUDE_NAMES = {
    "fixity_manifest.csv",
    "fixity_manifest.json",
    ".DS_Store",
}

def checksum(path: Path, algorithm: str) -> str:
    h = hashlib.new(algorithm)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def should_include(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name in EXCLUDE_NAMES:
        return False
    return True

def collect_files():
    paths = []

    for dirname in INCLUDE_DIRS:
        directory = ROOT / dirname
        if directory.exists():
            for path in directory.rglob("*"):
                if should_include(path):
                    paths.append(path)

    for filename in INCLUDE_ROOT_FILES:
        path = ROOT / filename
        if should_include(path):
            paths.append(path)

    return sorted(set(paths), key=lambda p: str(p).lower())

def main():
    generated_at = datetime.now(timezone.utc).isoformat()
    records = []

    for path in collect_files():
        rel_path = path.as_posix()
        record = {
            "path": rel_path,
            "size_bytes": path.stat().st_size,
            "md5": checksum(path, "md5"),
            "sha256": checksum(path, "sha256"),
            "generated_at_utc": generated_at,
        }
        records.append(record)

    csv_path = DOCS_DIR / "fixity_manifest.csv"
    json_path = DOCS_DIR / "fixity_manifest.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["path", "size_bytes", "md5", "sha256", "generated_at_utc"]
        )
        writer.writeheader()
        writer.writerows(records)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at_utc": generated_at,
                "file_count": len(records),
                "algorithm_notes": {
                    "md5": "Included because many repositories display MD5 checksums.",
                    "sha256": "Included as a stronger integrity check."
                },
                "files": records,
            },
            f,
            indent=2
        )

    print(f"Fixity manifest written: {csv_path}")
    print(f"Fixity manifest written: {json_path}")
    print(f"Files recorded: {len(records)}")

if __name__ == "__main__":
    main()
