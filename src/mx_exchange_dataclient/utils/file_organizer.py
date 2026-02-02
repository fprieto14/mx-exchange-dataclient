"""File organization utilities for downloaded documents.

Provides functions to organize documents into folders by type and
standardize filenames with date prefixes.
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

# Quarter end dates for standardized naming
QUARTER_DATES = {
    "1": "0331",
    "1T": "0331",
    "2": "0630",
    "2T": "0630",
    "3": "0930",
    "3T": "0930",
    "4": "1231",
    "4T": "1231",
    "4D": "1231",
    "4DT": "1231",
}

# Document type patterns and their folder names
DOC_TYPE_PATTERNS = [
    (r"ReporteTrimestral", "ReporteTrimestral"),
    (r"ReporteAnual", "ReporteAnual"),
    (r"bivaFR2", "ReporteAnual"),  # BIVA annual reports
    (r"EventoRelevante", "EventoRelevante"),
    (r"InformacionFinanciera", "InformacionFinanciera"),
    (r"InformacionCorporativa", "InformacionCorporativa"),
    (r"Prospecto", "Prospecto"),
    (r"Aviso", "Aviso"),
    (r"Dictamen", "Dictamen"),
]

# Default folder for unclassified documents
DEFAULT_DOC_TYPE = "Otros"

# Supported file extensions
SUPPORTED_EXTENSIONS = {
    ".xbrl",
    ".pdf",
    ".zip",
    ".html",
    ".docx",
    ".xlsx",
    ".xml",
    ".json",
    ".txt",
    ".csv",
}


def extract_doc_type(filename: str) -> str:
    """Extract document type from filename.

    Args:
        filename: Document filename

    Returns:
        Document type folder name
    """
    # Remove date prefix if present for matching
    clean_name = filename
    if re.match(r"^\d{8}_", filename):
        clean_name = filename[9:]

    for pattern, doc_type in DOC_TYPE_PATTERNS:
        if re.search(pattern, clean_name, re.IGNORECASE):
            return doc_type

    return DEFAULT_DOC_TYPE


def extract_date_from_filename(filename: str) -> str | None:
    """Extract date from filename patterns.

    Args:
        filename: Document filename

    Returns:
        Date string (YYYYMMDD) or None if not found
    """
    # Pattern 1: Already has date prefix like 20190603_...
    if re.match(r"^\d{8}_", filename):
        return filename[:8]

    # Pattern 2: ReporteTrimestral_[PERIOD]_[YEAR]_[ISSUER]_...
    match = re.match(r"ReporteTrimestral_(\d+(?:DT|T|D)?)_(\d{4})_", filename)
    if match:
        period, year = match.groups()
        if period in QUARTER_DATES:
            return f"{year}{QUARTER_DATES[period]}"

    # Pattern 3: ReporteTrimestral_[ISSUER]-[CODE]_[YEAR]_[QUARTER]_...
    match = re.match(r"ReporteTrimestral_[A-Z0-9]+-[A-Z0-9]+_(\d{4})_(\d+[D]?)_", filename)
    if match:
        year, period = match.groups()
        if period in QUARTER_DATES:
            return f"{year}{QUARTER_DATES[period]}"

    # Pattern 4: ReporteTrimestral_[ISSUER]_[YEAR]_[QUARTER]_...
    match = re.match(r"ReporteTrimestral_[A-Z0-9]+_(\d{4})_(\d+[D]?)_", filename)
    if match:
        year, period = match.groups()
        if period in QUARTER_DATES:
            return f"{year}{QUARTER_DATES[period]}"

    # Pattern 5: ReporteAnual_[YEAR]_...
    match = re.match(r"ReporteAnual_(\d{4})_", filename)
    if match:
        year = match.group(1)
        return f"{year}1231"

    # Pattern 6: ReporteAnual_[ISSUER]-[CODE]_[YEAR]_...
    match = re.match(r"ReporteAnual_[A-Z0-9]+-[A-Z0-9]+_(\d{4})_", filename)
    if match:
        year = match.group(1)
        return f"{year}1231"

    # Pattern 7: EventoRelevante_[ID]_[ISSUER]_[YEAR]_...
    match = re.match(
        r"EventoRelevante_\d+_[A-Z0-9]+_(\d{4})_(\d+)-(\d{2})(\d{2})(\d{2})-", filename
    )
    if match:
        year, _, yy, mm, dd = match.groups()
        full_year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
        return f"{full_year}{mm}{dd}"

    # Pattern 8: bivaFR2_19_... (assume 2019 for _19_)
    match = re.match(r"bivaFR2_(\d{2})_", filename)
    if match:
        year_short = match.group(1)
        year = f"20{year_short}" if int(year_short) < 50 else f"19{year_short}"
        return f"{year}1231"  # Default to year end

    # Pattern 9: Numeric PDF with date like [ID]-YYMMDD-[hash].pdf
    match = re.match(
        r"\d+-(\d{2})(\d{2})(\d{2})-[a-f0-9]+\.(pdf|xbrl|zip)$", filename, re.IGNORECASE
    )
    if match:
        yy, mm, dd = match.groups()[:3]
        year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
        if 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
            return f"{year}{mm}{dd}"

    # Pattern 10: PDF with date in name like YYYYMMDD somewhere
    match = re.search(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})", filename)
    if match:
        year, month, day = match.groups()
        if 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
            return f"{year}{month}{day}"

    return None


def standardize_filename(filename: str, fallback_date: str | None = None) -> str:
    """Standardize a filename with date prefix.

    Args:
        filename: Original filename
        fallback_date: Date to use if none found (YYYYMMDD format)

    Returns:
        Standardized filename with date prefix
    """
    # If already has date prefix, just clean up
    if re.match(r"^\d{8}_", filename):
        clean_name = filename[9:]  # Remove existing date prefix
        clean_name = re.sub(r"%20", "_", clean_name)
        clean_name = re.sub(r"[%\s]+", "_", clean_name)
        clean_name = re.sub(r"_+", "_", clean_name)
        return f"{filename[:8]}_{clean_name}"

    date_prefix = extract_date_from_filename(filename)

    if date_prefix is None:
        date_prefix = fallback_date or "00000000"

    # Clean up the filename
    clean_name = filename
    clean_name = re.sub(r"%20", "_", clean_name)
    clean_name = re.sub(r"[%\s]+", "_", clean_name)
    clean_name = re.sub(r"_+", "_", clean_name)

    return f"{date_prefix}_{clean_name}"


def get_file_mod_date(filepath: Path) -> str:
    """Get file modification date as YYYYMMDD.

    Args:
        filepath: Path to file

    Returns:
        Date string in YYYYMMDD format
    """
    mtime = os.path.getmtime(filepath)
    return datetime.fromtimestamp(mtime).strftime("%Y%m%d")


def organize_issuer_folder(
    issuer_path: Path,
    dry_run: bool = False,
) -> dict:
    """Organize files in a single issuer folder by document type.

    Args:
        issuer_path: Path to issuer folder
        dry_run: If True, don't actually move files

    Returns:
        Dict with statistics: moved, renamed, skipped, errors
    """
    stats = {"moved": 0, "renamed": 0, "skipped": 0, "errors": []}

    # Collect all files to process (at root level and in old extension-based folders)
    files_to_process = []

    # Files at root level
    for item in issuer_path.iterdir():
        if item.is_file() and item.suffix.lower() in SUPPORTED_EXTENSIONS:
            files_to_process.append(item)

    # Files in old extension-based folders (xbrls/, pdfs/, zips/)
    old_folders = ["xbrls", "pdfs", "zips"]
    for old_folder in old_folders:
        old_path = issuer_path / old_folder
        if old_path.exists() and old_path.is_dir():
            for item in old_path.iterdir():
                if item.is_file():
                    files_to_process.append(item)

    for item in files_to_process:
        # Skip macOS metadata files
        if item.name.startswith("._"):
            continue

        # Skip if file no longer exists (may have been moved already)
        if not item.exists():
            continue

        # Determine document type from filename
        doc_type = extract_doc_type(item.name)
        target_folder = issuer_path / doc_type

        # Create target folder if needed
        if not target_folder.exists() and not dry_run:
            target_folder.mkdir(parents=True)

        # Generate standardized name
        new_name = standardize_filename(item.name)

        # If date prefix is 00000000, use file mod date
        if new_name.startswith("00000000_"):
            mod_date = get_file_mod_date(item)
            new_name = mod_date + new_name[8:]

        target_path = target_folder / new_name

        # Skip if file is already in correct location with correct name
        if item.parent == target_folder and item.name == new_name:
            continue

        # Check if target already exists
        if target_path.exists():
            # Try with a suffix
            base, file_ext = os.path.splitext(new_name)
            for i in range(1, 100):
                alt_name = f"{base}_{i}{file_ext}"
                target_path = target_folder / alt_name
                if not target_path.exists():
                    break
            else:
                stats["errors"].append(f"Could not find unique name for {item.name}")
                stats["skipped"] += 1
                continue

        if dry_run:
            rel_source = item.relative_to(issuer_path) if item.parent != issuer_path else item.name
            print(f"  Would move: {rel_source}")
            print(f"         -> {doc_type}/{target_path.name}")
        else:
            try:
                shutil.move(str(item), str(target_path))
                stats["moved"] += 1
            except Exception as e:
                stats["errors"].append(f"Error moving {item.name}: {e}")

    # Clean up old empty extension-based folders
    for old_folder in old_folders:
        old_path = issuer_path / old_folder
        if old_path.exists() and old_path.is_dir():
            try:
                # Only remove if empty
                if not any(old_path.iterdir()):
                    if not dry_run:
                        old_path.rmdir()
                    else:
                        print(f"  Would remove empty folder: {old_folder}/")
            except OSError:
                pass  # Folder not empty, leave it

    return stats


def organize_output_folder(output_dir: Path, dry_run: bool = False) -> dict:
    """Organize all issuer folders in output directory.

    Args:
        output_dir: Path to output directory containing issuer folders
        dry_run: If True, don't actually move files

    Returns:
        Dict with aggregate statistics
    """
    total_stats = {"moved": 0, "renamed": 0, "skipped": 0, "errors": []}

    for issuer_dir in sorted(output_dir.iterdir()):
        if not issuer_dir.is_dir():
            continue
        if issuer_dir.name.startswith("."):
            continue

        print(f"\nProcessing {issuer_dir.name}...")
        stats = organize_issuer_folder(issuer_dir, dry_run=dry_run)

        for key in ["moved", "renamed", "skipped"]:
            total_stats[key] += stats[key]
        total_stats["errors"].extend(stats["errors"])

        if stats["moved"] > 0:
            print(f"  Moved: {stats['moved']} files")
        if stats["errors"]:
            for err in stats["errors"]:
                print(f"  Error: {err}")

    return total_stats
