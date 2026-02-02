#!/usr/bin/env python3
"""Download latest quarterly XBRL for all CERPIs, organized by ticker folder."""

import json
import time
from pathlib import Path

import requests

BASE_URL = "https://www.biva.mx/emisoras"
STORAGE_BASE = "https://biva.mx"
OUTPUT_DIR = Path("output")

session = requests.Session()
session.headers.update({
    "User-Agent": "biva-client/0.1.0",
    "Accept": "application/json",
    "Referer": "https://www.biva.mx/empresas/emisoras_inscritas",
})


def get_cerpis() -> list[dict]:
    """Get all CERPIs from BIVA API."""
    resp = session.get(f"{BASE_URL}/empresas", params={"tipoInstrumento": 3, "page": 0, "size": 500})
    resp.raise_for_status()
    data = resp.json()
    return [item for item in data["content"] if item["clave"].endswith("PI")]


def get_latest_xbrl_doc(issuer_id: int) -> dict | None:
    """Get the latest quarterly XBRL document for an issuer."""
    resp = session.get(
        f"{BASE_URL}/empresas/{issuer_id}/documentos",
        params={"tipoInformacion": "17", "page": 0, "size": 20}
    )
    resp.raise_for_status()
    data = resp.json()

    for doc in data["content"]:
        if doc["docType"] == "XBRL" and "trimestral" in doc["tipoDocumento"].lower():
            return doc
    return None


def get_xbrl_url(doc: dict) -> str | None:
    """Extract XBRL file URL from document."""
    for f in doc.get("archivosXbrl", []):
        if f.get("extension", "").upper() == "XBRL":
            url = f["url"]
            if url.startswith("/"):
                return f"{STORAGE_BASE}{url}"
            return url
    return None


def download_xbrl(ticker: str, doc: dict) -> Path | None:
    """Download XBRL file to ticker folder."""
    url = get_xbrl_url(doc)
    if not url:
        return None

    ticker_dir = OUTPUT_DIR / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    filename = doc["fileName"]
    filepath = ticker_dir / filename

    if filepath.exists():
        print(f"  Already exists: {filename}")
        return filepath

    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        filepath.write_bytes(resp.content)
        print(f"  Downloaded: {filename}")
        return filepath
    except Exception as e:
        print(f"  ERROR downloading: {e}")
        return None


def main():
    print("Fetching CERPI list...")
    cerpis = get_cerpis()
    print(f"Found {len(cerpis)} CERPIs\n")

    results = []

    for cerpi in cerpis:
        ticker = cerpi["clave"]
        issuer_id = cerpi["id"]
        print(f"{ticker} (ID: {issuer_id})")

        doc = get_latest_xbrl_doc(issuer_id)
        if not doc:
            print("  No quarterly XBRL found")
            results.append({"ticker": ticker, "id": issuer_id, "status": "no_xbrl", "file": None, "period": None})
            time.sleep(0.3)
            continue

        period = doc["tipoDocumento"]
        filepath = download_xbrl(ticker, doc)

        results.append({
            "ticker": ticker,
            "id": issuer_id,
            "status": "downloaded" if filepath else "failed",
            "file": str(filepath) if filepath else None,
            "period": period,
        })

        time.sleep(0.3)

    # Save results summary
    summary_path = OUTPUT_DIR / "cerpi_xbrl_downloads.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSummary saved to {summary_path}")

    # Print stats
    downloaded = sum(1 for r in results if r["status"] == "downloaded")
    no_xbrl = sum(1 for r in results if r["status"] == "no_xbrl")
    failed = sum(1 for r in results if r["status"] == "failed")
    print(f"\nResults: {downloaded} downloaded, {no_xbrl} no XBRL, {failed} failed")


if __name__ == "__main__":
    main()
