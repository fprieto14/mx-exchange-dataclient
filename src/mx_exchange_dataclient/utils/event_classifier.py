"""Event document classification utilities.

Classifies EventoRelevante documents into subcategories:
- LlamadasCapitalDistribuciones: Capital calls, distributions, fund liquidations
- AvisosSuscripcion: Subscription notices, additional placements, reopenings
- OtrosEventosRelevantes: Everything else
"""

import re
import shutil
import subprocess
from pathlib import Path

# Keywords for classification (case-insensitive)
CAPITAL_CALL_KEYWORDS = [
    r"llamada de capital",
    r"liquidaci[oó]n.*fondo",
    r"fondeo.*inversi[oó]n",
    r"distribuci[oó]n.*tenedores",
    r"distribuci[oó]n.*efectivo",
    r"aviso de distribuci[oó]n",
    r"pago.*distribuci[oó]n",
    r"reembolso.*capital",
]

SUBSCRIPTION_KEYWORDS = [
    r"aviso de suscripci[oó]n",
    r"colocaci[oó]n subsecuente",
    r"colocaci[oó]n adicional",
    r"notificaci[oó]n de colocaci[oó]n",
    r"emisi[oó]n adicional",
    r"reapertura",
    r"suscripci[oó]n.*pago",
    r"suscripci[oó]n preferente",
]

# Category names
CATEGORY_CAPITAL_CALLS = "LlamadasCapitalDistribuciones"
CATEGORY_SUBSCRIPTIONS = "AvisosSuscripcion"
CATEGORY_OTHER = "OtrosEventosRelevantes"


def extract_text_from_pdf(pdf_path: Path, max_chars: int = 3000) -> str:
    """Extract text from PDF using pdftotext.

    Args:
        pdf_path: Path to PDF file
        max_chars: Maximum characters to extract

    Returns:
        Extracted text or empty string if failed
    """
    try:
        result = subprocess.run(
            ["pdftotext", "-l", "3", str(pdf_path), "-"],  # First 3 pages
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout[:max_chars] if result.stdout else ""
    except Exception:
        return ""


def classify_document(text: str) -> str:
    """Classify document based on text content.

    Args:
        text: Document text content

    Returns:
        Category name (LlamadasCapitalDistribuciones, AvisosSuscripcion, or OtrosEventosRelevantes)
    """
    text_lower = text.lower()

    # Check for capital call/distribution keywords
    for pattern in CAPITAL_CALL_KEYWORDS:
        if re.search(pattern, text_lower):
            return CATEGORY_CAPITAL_CALLS

    # Check for subscription keywords
    for pattern in SUBSCRIPTION_KEYWORDS:
        if re.search(pattern, text_lower):
            return CATEGORY_SUBSCRIPTIONS

    # Default to other
    return CATEGORY_OTHER


def classify_pdf(pdf_path: Path) -> str:
    """Classify a PDF document by extracting and analyzing its text.

    Args:
        pdf_path: Path to PDF file

    Returns:
        Category name
    """
    text = extract_text_from_pdf(pdf_path)
    return classify_document(text)


def get_related_files(pdf_path: Path) -> list[Path]:
    """Get all related files (same base name, different extensions).

    Args:
        pdf_path: Path to PDF file

    Returns:
        List of related file paths
    """
    base_name = pdf_path.stem
    parent = pdf_path.parent
    related = []

    for ext in [".pdf", ".html", ".docx", ".xlsx"]:
        file_path = parent / f"{base_name}{ext}"
        if file_path.exists():
            related.append(file_path)

    return related


def process_evento_folder(
    issuer_path: Path,
    dry_run: bool = False,
) -> dict:
    """Process EventoRelevante folder and classify documents.

    Args:
        issuer_path: Path to issuer folder
        dry_run: If True, don't actually move files

    Returns:
        Dict with counts per category
    """
    stats = {
        CATEGORY_CAPITAL_CALLS: 0,
        CATEGORY_SUBSCRIPTIONS: 0,
        CATEGORY_OTHER: 0,
    }

    evento_dir = issuer_path / "EventoRelevante"
    otros_dir = issuer_path / "Otros"

    # Create target directories
    for category in stats.keys():
        target_dir = issuer_path / category
        if not dry_run:
            target_dir.mkdir(exist_ok=True)

    processed_bases = set()

    # Process EventoRelevante folder
    if evento_dir.exists():
        for item in evento_dir.iterdir():
            if (
                item.is_file()
                and item.suffix.lower() == ".pdf"
                and not item.name.startswith("._")
            ):
                base_name = item.stem
                if base_name in processed_bases:
                    continue
                processed_bases.add(base_name)

                # Extract text and classify
                text = extract_text_from_pdf(item)
                category = classify_document(text)

                # Move all related files
                related_files = get_related_files(item)
                for f in related_files:
                    target = issuer_path / category / f.name
                    if not target.exists():
                        if dry_run:
                            print(f"  Would move: {f.name} -> {category}/")
                        else:
                            shutil.move(str(f), str(target))
                        stats[category] += 1

    # Process Otros folder (files without document type prefix)
    if otros_dir.exists():
        for item in otros_dir.iterdir():
            if (
                item.is_file()
                and item.suffix.lower() == ".pdf"
                and not item.name.startswith("._")
            ):
                base_name = item.stem
                if base_name in processed_bases:
                    continue
                processed_bases.add(base_name)

                # Extract text and classify
                text = extract_text_from_pdf(item)
                category = classify_document(text)

                # Move all related files
                related_files = get_related_files(item)
                for f in related_files:
                    target = issuer_path / category / f.name
                    if not target.exists():
                        if dry_run:
                            print(f"  Would move: {f.name} -> {category}/")
                        else:
                            shutil.move(str(f), str(target))
                        stats[category] += 1

    # Clean up empty EventoRelevante and Otros folders
    if not dry_run:
        for folder in [evento_dir, otros_dir]:
            if folder.exists():
                remaining = [f for f in folder.iterdir() if not f.name.startswith("._")]
                if not remaining:
                    shutil.rmtree(folder, ignore_errors=True)

    return stats


def classify_events_in_output(output_dir: Path, dry_run: bool = False) -> dict:
    """Classify event documents in all issuer folders.

    Args:
        output_dir: Path to output directory
        dry_run: If True, don't actually move files

    Returns:
        Dict with aggregate counts
    """
    total_stats = {
        CATEGORY_CAPITAL_CALLS: 0,
        CATEGORY_SUBSCRIPTIONS: 0,
        CATEGORY_OTHER: 0,
    }

    for issuer_dir in sorted(output_dir.iterdir()):
        if issuer_dir.is_dir() and not issuer_dir.name.startswith("."):
            print(f"\nProcessing {issuer_dir.name}...")
            stats = process_evento_folder(issuer_dir, dry_run=dry_run)

            for k, v in stats.items():
                total_stats[k] += v

            if any(stats.values()):
                for k, v in stats.items():
                    if v > 0:
                        print(f"  {k}: {v} files")

    return total_stats
