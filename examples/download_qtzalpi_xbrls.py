#!/usr/bin/env python3
"""Download XBRL files for QTZALPI from BIVA."""

import sys
sys.path.insert(0, 'src')

from pathlib import Path
from mx_exchange_dataclient.client import BIVAClient

ISSUER_ID = 2282  # QTZALPI
OUTPUT_DIR = Path("output/QTZALPI/xbrls")


def main():
    client = BIVAClient()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching documents for QTZALPI (ID: {ISSUER_ID})...")

    # Get all documents
    xbrl_docs = []
    for doc in client.iter_documents(ISSUER_ID):
        # Filter XBRL files (usually .xbrl or inside zip files)
        fname = doc.file_name.lower()
        if fname.endswith('.xbrl') or fname.endswith('.zip'):
            xbrl_docs.append(doc)

    print(f"Found {len(xbrl_docs)} XBRL/ZIP files")

    # Download each
    downloaded = 0
    for i, doc in enumerate(xbrl_docs, 1):
        print(f"[{i}/{len(xbrl_docs)}] {doc.tipo_documento}: {doc.file_name}")
        path = client.download_document(doc, OUTPUT_DIR, delay=0.3)
        if path:
            downloaded += 1

    print(f"\nDownloaded {downloaded} files to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
