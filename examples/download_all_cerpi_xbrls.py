#!/usr/bin/env python3
"""Download ALL quarterly XBRL files for CERPIs, ordered by fund size."""

import json
import time
from pathlib import Path

import requests

BASE_URL = "https://www.biva.mx/emisoras"
STORAGE_BASE = "https://biva.mx"
OUTPUT_DIR = Path("output")

# Rate limiting - conservative to avoid blocks
DELAY_BETWEEN_REQUESTS = 0.5  # seconds between API calls
DELAY_BETWEEN_DOWNLOADS = 1.0  # seconds between file downloads
DELAY_BETWEEN_ISSUERS = 2.0   # seconds between different issuers

session = requests.Session()
session.headers.update({
    "User-Agent": "biva-client/0.1.0",
    "Accept": "application/json",
    "Referer": "https://www.biva.mx/empresas/emisoras_inscritas",
})


def get_all_xbrl_docs(issuer_id: int) -> list[dict]:
    """Get all quarterly XBRL documents for an issuer."""
    all_docs = []
    page = 0

    while True:
        time.sleep(DELAY_BETWEEN_REQUESTS)
        resp = session.get(
            f"{BASE_URL}/empresas/{issuer_id}/documentos",
            params={"tipoInformacion": "17", "page": page, "size": 100}
        )
        resp.raise_for_status()
        data = resp.json()

        # Filter for XBRL documents only
        xbrl_docs = [
            doc for doc in data["content"]
            if doc["docType"] == "XBRL" and "trimestral" in doc["tipoDocumento"].lower()
        ]
        all_docs.extend(xbrl_docs)

        if page + 1 >= data["totalPages"]:
            break
        page += 1

    return all_docs


def get_xbrl_url(doc: dict) -> str | None:
    """Extract XBRL file URL from document."""
    for f in doc.get("archivosXbrl", []):
        if f.get("extension", "").upper() == "XBRL":
            url = f["url"]
            if url.startswith("/"):
                return f"{STORAGE_BASE}{url}"
            return url
    return None


def download_xbrl(ticker: str, doc: dict) -> tuple[Path | None, bool]:
    """Download XBRL file. Returns (path, was_new)."""
    url = get_xbrl_url(doc)
    if not url:
        return None, False

    ticker_dir = OUTPUT_DIR / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    filename = doc["fileName"]
    filepath = ticker_dir / filename

    if filepath.exists():
        return filepath, False  # Already had it

    try:
        time.sleep(DELAY_BETWEEN_DOWNLOADS)
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        filepath.write_bytes(resp.content)
        return filepath, True  # New download
    except Exception as e:
        print(f"      ERROR: {e}")
        return None, False


def main():
    # Load size-ordered list
    sizes_path = OUTPUT_DIR / "cerpi_sizes.json"
    with open(sizes_path) as f:
        cerpis = json.load(f)

    print(f"Downloading all XBRLs for {len(cerpis)} CERPIs (ordered by size)")
    print(f"Rate limits: {DELAY_BETWEEN_REQUESTS}s/request, {DELAY_BETWEEN_DOWNLOADS}s/download")
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

        # Get all XBRL documents
        try:
            docs = get_all_xbrl_docs(issuer_id)
            print(f"  Found {len(docs)} quarterly XBRL documents")
        except Exception as e:
            print(f"  ERROR fetching documents: {e}")
            continue

        # Download each
        new_count = 0
        existing_count = 0
        failed_count = 0

        for doc in docs:
            periodo = doc["tipoDocumento"]
            filepath, was_new = download_xbrl(ticker, doc)

            if filepath:
                if was_new:
                    print(f"    + {doc['fileName']}")
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
