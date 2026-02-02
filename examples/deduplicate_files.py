#!/usr/bin/env python3
"""Deduplicate files by content hash, replacing duplicates with symlinks."""

import hashlib
import os
from collections import defaultdict
from pathlib import Path

OUTPUT_DIR = Path("output")


def md5_file(filepath: Path) -> str:
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    print("Scanning files...")

    # Group files by hash
    hash_to_files: dict[str, list[Path]] = defaultdict(list)

    extensions = (".pdf", ".html", ".docx", ".xlsx", ".xls", ".doc", ".htm")
    all_files = []

    for ext in extensions:
        all_files.extend(OUTPUT_DIR.rglob(f"*{ext}"))

    print(f"Found {len(all_files)} files to check")

    for i, filepath in enumerate(all_files):
        if filepath.is_symlink():
            continue  # Skip existing symlinks

        file_hash = md5_file(filepath)
        hash_to_files[file_hash].append(filepath)

        if (i + 1) % 1000 == 0:
            print(f"  Hashed {i + 1}/{len(all_files)} files...")

    # Find duplicates
    duplicates = {h: files for h, files in hash_to_files.items() if len(files) > 1}

    total_dups = sum(len(files) - 1 for files in duplicates.values())
    print(f"\nFound {len(duplicates)} unique hashes with duplicates")
    print(f"Total duplicate files: {total_dups}")

    # Calculate space savings
    space_saved = 0
    for files in duplicates.values():
        original = files[0]
        for dup in files[1:]:
            space_saved += dup.stat().st_size

    print(f"Potential space savings: {space_saved / 1e9:.2f} GB")

    # Replace duplicates with symlinks
    print("\nReplacing duplicates with symlinks...")
    replaced = 0
    errors = 0

    for file_hash, files in duplicates.items():
        # Keep the first file as the original
        original = files[0]

        for dup in files[1:]:
            try:
                # Calculate relative path from dup to original
                rel_path = os.path.relpath(original, dup.parent)

                # Remove the duplicate
                dup.unlink()

                # Create symlink
                dup.symlink_to(rel_path)
                replaced += 1

            except Exception as e:
                print(f"  Error with {dup}: {e}")
                errors += 1

        if replaced % 500 == 0 and replaced > 0:
            print(f"  Replaced {replaced} files...")

    print(f"\nDone!")
    print(f"  Replaced: {replaced} files with symlinks")
    print(f"  Errors: {errors}")
    print(f"  Space saved: {space_saved / 1e9:.2f} GB")

    # Verify
    print("\nVerifying disk usage...")
    os.system(f"du -sh {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
