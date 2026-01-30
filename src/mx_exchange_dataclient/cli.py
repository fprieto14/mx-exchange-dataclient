"""
BIVA Client CLI

Command-line interface for fetching BIVA data.

Usage:
    biva issuer 2215
    biva documents 2215 --output ./capglpi
    biva output 2215 --output ./capglpi/pdfs
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from biva_client import BIVAClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_issuer(args):
    """Get issuer information."""
    client = BIVAClient()

    issuer = client.get_issuer(args.issuer_id)

    if args.json:
        print(issuer.model_dump_json(indent=2))
        return

    print(f"\n{'=' * 50}")
    print(f"ISSUER: {issuer.clave}")
    print(f"{'=' * 50}")
    print(f"ID:           {issuer.id}")
    print(f"Razón Social: {issuer.razon_social}")
    print(f"Sector:       {issuer.sector.nombre if issuer.sector else 'N/A'}")
    print(f"Status:       {issuer.estatus}")
    print(f"Website:      {issuer.sitio_web or 'N/A'}")
    print(f"Phone:        {issuer.telefono or 'N/A'}")

    if args.securities:
        print(f"\n{'=' * 50}")
        print("SECURITIES")
        print(f"{'=' * 50}")
        securities = client.get_issuer_securities(args.issuer_id)
        for sec in securities:
            print(f"  - {sec.nombre} ({sec.isin})")

    if args.emissions:
        print(f"\n{'=' * 50}")
        print("EMISSIONS")
        print(f"{'=' * 50}")
        emissions = client.get_all_emissions(args.issuer_id)
        for em in emissions:
            print(f"  - Serie {em.serie}: {em.nombre}")
            print(f"    ISIN: {em.isin}")
            print(f"    Type: {em.tipo_instrumento}")
            if em.titulos_en_circulacion:
                print(f"    Titles: {em.titulos_en_circulacion:,}")


def cmd_documents(args):
    """List documents for an issuer."""
    client = BIVAClient()

    # Get document count first
    total = client.get_document_count(args.issuer_id)
    print(f"Found {total} documents for issuer {args.issuer_id}")

    # Get document types
    if args.types:
        doc_types = client.get_document_types(args.issuer_id)
        print("\nDocument Types:")
        for dt in doc_types:
            print(f"  - [{dt.id}] {dt.nombre}")
        return

    # Fetch documents
    max_pages = args.max_pages or (1 if not args.all else None)
    documents = client.get_all_documents(args.issuer_id, max_pages=max_pages)

    if args.json:
        print(json.dumps([d.model_dump() for d in documents], indent=2, default=str))
        return

    # Convert to DataFrame
    records = []
    for doc in documents:
        records.append({
            "id": doc.id,
            "tipo_documento": doc.tipo_documento,
            "fecha_publicacion": doc.fecha_publicacion,
            "filename": doc.file_name,
            "download_url": doc.download_url,
        })

    df = pd.DataFrame(records)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Saved {len(df)} records to {output_path}")
    else:
        print(f"\nShowing first {min(20, len(df))} of {len(df)} documents:")
        print(df.head(20).to_string(index=False))


def cmd_download(args):
    """Download documents for an issuer."""
    client = BIVAClient()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    total = client.get_document_count(args.issuer_id)
    print(f"Downloading {total} documents to {output_dir}")

    def progress(current, total):
        if current % 50 == 0 or current == total:
            pct = current / total * 100
            print(f"  Progress: {current}/{total} ({pct:.1f}%)")

    downloaded = client.download_all_documents(
        args.issuer_id,
        output_dir,
        max_pages=args.max_pages,
        progress_callback=progress,
    )

    print(f"\nDownloaded {len(downloaded)} files to {output_dir}")


def cmd_export(args):
    """Export full issuer data (info + documents) to JSON."""
    client = BIVAClient()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Exporting data for issuer {args.issuer_id}...")

    # Fetch all data
    issuer = client.get_issuer(args.issuer_id)
    securities = client.get_issuer_securities(args.issuer_id)
    emissions = client.get_all_emissions(args.issuer_id)
    doc_types = client.get_document_types(args.issuer_id)
    documents = client.get_all_documents(args.issuer_id, max_pages=args.max_pages)

    # Save issuer info
    issuer_data = {
        "issuer": issuer.model_dump(),
        "securities": [s.model_dump() for s in securities],
        "emissions": [e.model_dump() for e in emissions],
        "document_types": [dt.model_dump() for dt in doc_types],
    }

    issuer_file = output_dir / "issuer_info.json"
    with open(issuer_file, "w", encoding="utf-8") as f:
        json.dump(issuer_data, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved issuer info to {issuer_file}")

    # Save documents
    doc_records = []
    for doc in documents:
        doc_records.append({
            "id": doc.id,
            "tipo_documento": doc.tipo_documento,
            "fecha_publicacion": doc.fecha_publicacion.isoformat() if doc.fecha_publicacion else None,
            "filename": doc.file_name,
            "download_url": doc.download_url,
            "doc_type": doc.doc_type,
        })

    df = pd.DataFrame(doc_records)
    docs_file = output_dir / "documents.csv"
    df.to_csv(docs_file, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} document records to {docs_file}")

    # Download if requested
    if args.download:
        pdfs_dir = output_dir / "pdfs"
        print(f"\nDownloading {len(documents)} documents...")

        downloaded = client.download_all_documents(
            args.issuer_id,
            pdfs_dir,
            max_pages=args.max_pages,
        )
        print(f"Downloaded {len(downloaded)} files")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="biva",
        description="BIVA (Bolsa Institucional de Valores) data client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get issuer info
  biva issuer 2215
  biva issuer CAPGLPI --securities --emissions

  # List documents
  biva documents 2215 --types
  biva documents 2215 --all --output documents.csv

  # Download all documents
  biva output 2215 --output ./downloads/capglpi

  # Full export (info + documents + PDFs)
  biva export 2215 --output ./capglpi --output
        """,
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # issuer command
    issuer_parser = subparsers.add_parser("issuer", help="Get issuer information")
    issuer_parser.add_argument("issuer_id", help="Issuer ID or name (e.g., 2215 or CAPGLPI)")
    issuer_parser.add_argument("--securities", action="store_true", help="Include securities")
    issuer_parser.add_argument("--emissions", action="store_true", help="Include emissions")
    issuer_parser.add_argument("--json", action="store_true", help="Output as JSON")
    issuer_parser.set_defaults(func=cmd_issuer)

    # documents command
    docs_parser = subparsers.add_parser("documents", help="List issuer documents")
    docs_parser.add_argument("issuer_id", help="Issuer ID or name")
    docs_parser.add_argument("--output", "-o", help="Output CSV file")
    docs_parser.add_argument("--all", action="store_true", help="Fetch all pages")
    docs_parser.add_argument("--max-pages", type=int, help="Limit pages")
    docs_parser.add_argument("--types", action="store_true", help="List document types only")
    docs_parser.add_argument("--json", action="store_true", help="Output as JSON")
    docs_parser.set_defaults(func=cmd_documents)

    # output command
    dl_parser = subparsers.add_parser("output", help="Download documents")
    dl_parser.add_argument("issuer_id", help="Issuer ID or name")
    dl_parser.add_argument("--output", "-o", required=True, help="Output directory")
    dl_parser.add_argument("--max-pages", type=int, help="Limit pages")
    dl_parser.set_defaults(func=cmd_download)

    # export command
    export_parser = subparsers.add_parser("export", help="Export full issuer data")
    export_parser.add_argument("issuer_id", help="Issuer ID or name")
    export_parser.add_argument("--output", "-o", required=True, help="Output directory")
    export_parser.add_argument("--output", "-d", action="store_true", help="Also output PDFs")
    export_parser.add_argument("--max-pages", type=int, help="Limit document pages")
    export_parser.set_defaults(func=cmd_export)

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        # Convert issuer_id to int if numeric
        if hasattr(args, "issuer_id"):
            try:
                args.issuer_id = int(args.issuer_id)
            except ValueError:
                pass  # Keep as string (name)

        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
