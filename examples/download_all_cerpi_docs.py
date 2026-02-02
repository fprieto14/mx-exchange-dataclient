#!/usr/bin/env python3
"""Download ALL documents (PDF, ZIP, DOCX, etc.) for CERPIs, ordered by fund size."""

import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

BASE_URL = "https://www.biva.mx/emisoras"
STORAGE_BASE = "https://biva.mx"
OUTPUT_DIR = Path("output")

# Rate limiting
DELAY_BETWEEN_REQUESTS = 0.5
DELAY_BETWEEN_DOWNLOADS = 0.8
DELAY_BETWEEN_ISSUERS = 2.0

session = requests.Session()
session.headers.update({
    "User-Agent": "biva-client/0.1.0",
    "Accept": "application/json",
    "Referer": "https://www.biva.mx/empresas/emisoras_inscritas",
})


def get_all_documents(issuer_id: int) -> list[dict]:
    """Get ALL documents for an issuer (all types)."""
    all_docs = []
    page = 0

    while True:
        time.sleep(DELAY_BETWEEN_REQUESTS)
        resp = session.get(
            f"{BASE_URL}/empresas/{issuer_id}/documentos",
            params={"page": page, "size": 100}
        )
        resp.raise_for_status()
        data = resp.json()

        all_docs.extend(data["content"])

        if page + 1 >= data["totalPages"]:
            break
        page += 1

    return all_docs


def extract_download_urls(doc: dict) -> list[tuple[str, str]]:
    """Extract all downloadable URLs from a document. Returns list of (url, filename)."""
    urls = []
    base = STORAGE_BASE

    # Main document URL
    nombre_archivo = doc.get("nombreArchivo", "")
    file_name = doc.get("fileName", "")
    doc_type = doc.get("docType", "").lower()

    # Handle XBRL viewer URLs - extract actual file paths
    if "visorxbrl/index.html" in nombre_archivo:
        parsed = urlparse(nombre_archivo)
        params = parse_qs(parsed.query)

        # Extract all file paths from query params
        for key, values in params.items():
            if key.startswith("documentPath") and values:
                path = values[0]
                if path.startswith("/"):
                    url = f"{base}{path}"
                else:
                    url = f"{base}/{path}"
                fname = Path(path).name
                urls.append((url, fname))
    elif nombre_archivo:
        # Regular file path
        if nombre_archivo.startswith("/"):
            url = f"{base}{nombre_archivo}"
        else:
            url = f"{base}/{nombre_archivo}"
        urls.append((url, file_name))

    # Additional files from archivos list
    for f in doc.get("archivos", []):
        file_url = f.get("url", "")
        fname = f.get("fileName", "")
        if file_url and "{contexto" not in file_url:  # Skip template URLs
            if file_url.startswith("/"):
                urls.append((f"{base}{file_url}", fname))

    # Additional files from archivosXbrl list (already handled for XBRL, but get other formats)
    for f in doc.get("archivosXbrl", []):
        file_url = f.get("url", "")
        ext = f.get("extension", "").lower()
        if file_url and ext != "xbrl":  # Skip XBRL, already downloaded
            if file_url.startswith("/"):
                url = f"{base}{file_url}"
                fname = Path(file_url).name
                urls.append((url, fname))

    return urls


def download_file(ticker: str, url: str, filename: str) -> tuple[Path | None, bool]:
    """Download a file. Returns (path, was_new)."""
    ticker_dir = OUTPUT_DIR / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    # Clean filename
    if not filename:
        filename = Path(urlparse(url).path).name

    # Skip if no valid filename
    if not filename or filename in ("index.html", ""):
        return None, False

    filepath = ticker_dir / filename

    # Skip if exists
    if filepath.exists():
        return filepath, False

    try:
        time.sleep(DELAY_BETWEEN_DOWNLOADS)
        resp = session.get(url, timeout=120, stream=True)
        resp.raise_for_status()

        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return filepath, True
    except Exception as e:
        print(f"      ERROR {filename}: {e}")
        return None, False


def main():
    # Load size-ordered list
    sizes_path = OUTPUT_DIR / "cerpi_sizes.json"
    with open(sizes_path) as f:
        cerpis = json.load(f)

    print(f"Downloading ALL documents for {len(cerpis)} CERPIs (ordered by size)")
    print(f"Rate limits: {DELAY_BETWEEN_REQUESTS}s/request, {DELAY_BETWEEN_DOWNLOADS}s/download")
    print("Skipping: XBRL files (already downloaded)")
    print("=" * 70)

    total_new = 0
    total_existing = 0
    total_failed = 0

    for i, cerpi in enumerate(cerpis, 1):
        ticker = cerpi["ticker"]
        issuer_id = cerpi["id"]
        assets_usd = cerpi.get("assets_usd") or 0
        size_str = f"${assets_usd/1e9:.2f}B" if assets_usd >= 1e9 else f"${assets_usd/1e6:.0f}M"

        print(f"\n[{i}/{len(cerpis)}] {ticker} (ID: {issuer_id}) - {size_str} USD")

        # Get all documents
        try:
            docs = get_all_documents(issuer_id)
            print(f"  Found {len(docs)} total documents")
        except Exception as e:
            print(f"  ERROR fetching documents: {e}")
            continue

        # Download each document's files
        new_count = 0
        existing_count = 0
        failed_count = 0
        downloaded_urls = set()  # Avoid duplicates

        for doc in docs:
            urls = extract_download_urls(doc)

            for url, filename in urls:
                # Skip duplicates and XBRL files
                if url in downloaded_urls:
                    continue
                if filename and filename.endswith(".xbrl"):
                    continue

                downloaded_urls.add(url)
                filepath, was_new = download_file(ticker, url, filename)

                if filepath:
                    if was_new:
                        ext = filepath.suffix.lower()
                        print(f"    + [{ext}] {filepath.name[:60]}")
                        new_count += 1
                    else:
                        existing_count += 1
                else:
                    failed_count += 1

        if existing_count > 0:
            print(f"  ({existing_count} already downloaded)")
        if failed_count > 0:
            print(f"  ({failed_count} failed)")

        total_new += new_count
        total_existing += existing_count
        total_failed += failed_count

        # Delay between issuers
        if i < len(cerpis):
            time.sleep(DELAY_BETWEEN_ISSUERS)

    print("\n" + "=" * 70)
    print(f"COMPLETE: {total_new} new, {total_existing} existing, {total_failed} failed")


if __name__ == "__main__":
    main()
