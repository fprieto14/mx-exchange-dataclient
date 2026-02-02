"""
BMV Client CLI

Command-line interface for fetching BMV (Bolsa Mexicana de Valores) data.

Usage:
    bmv issuer LOCKXPI 35563
    bmv documents LOCKXPI 35563 CGEN_CAPIT
    bmv download LOCKXPI 35563 CGEN_CAPIT --output ./downloads
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from mx_exchange_dataclient.clients.bmv import BMVClient
from mx_exchange_dataclient.models.bmv import MARKET_TYPES, resolve_bmv_issuer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_issuer(args):
    """Get issuer information."""
    client = BMVClient()

    # Resolve ticker and ID
    if args.issuer_id:
        ticker = args.ticker
        issuer_id = args.issuer_id
    else:
        ticker, issuer_id = resolve_bmv_issuer(args.ticker)

    issuer = client.get_issuer(ticker, issuer_id)

    if args.json:
        print(issuer.model_dump_json(indent=2))
        return

    print(f"\n{'=' * 50}")
    print(f"ISSUER: {issuer.ticker}")
    print(f"{'=' * 50}")
    print(f"ID:      {issuer.id}")
    print(f"Name:    {issuer.name}")
    print(f"Market:  {issuer.market} ({MARKET_TYPES.get(issuer.market, 'Unknown')})")
    print(f"Status:  {issuer.status}")
    print(f"URL:     {issuer.profile_url}")

    if args.securities:
        print(f"\n{'=' * 50}")
        print("SECURITIES")
        print(f"{'=' * 50}")
        securities = client.get_issuer_securities(ticker, issuer_id)
        if securities:
            for sec in securities:
                print(f"  - {sec.name} (Series: {sec.series})")
        else:
            print("  No securities found")


def cmd_documents(args):
    """List documents for an issuer."""
    client = BMVClient()

    # Resolve ticker and ID
    if args.issuer_id:
        ticker = args.ticker
        issuer_id = args.issuer_id
    else:
        ticker, issuer_id = resolve_bmv_issuer(args.ticker)

    market = args.market

    # Get documents by category
    if args.category == "financial":
        documents = client.get_financial_documents(ticker, issuer_id, market)
    elif args.category == "events":
        documents = client.get_relevant_events(ticker, issuer_id, market)
    elif args.category == "corporate":
        documents = client.get_corporate_documents(ticker, issuer_id, market)
    else:
        documents = client.get_all_documents(ticker, issuer_id, market)

    print(f"Found {len(documents)} documents for {ticker}-{issuer_id}")

    if args.json:
        print(json.dumps([d.model_dump() for d in documents], indent=2, default=str))
        return

    # Convert to DataFrame
    records = []
    for doc in documents:
        records.append({
            "id": doc.id,
            "category": doc.category,
            "doc_type": doc.doc_type,
            "period": doc.period,
            "filename": doc.filename,
            "is_xbrl": doc.is_xbrl,
            "download_url": doc.download_url,
        })

    df = pd.DataFrame(records)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Saved {len(df)} records to {output_path}")
    else:
        if len(df) > 0:
            print(f"\nShowing first {min(20, len(df))} of {len(df)} documents:")
            print(df.head(20).to_string(index=False))
        else:
            print("\nNo documents found")


def cmd_download(args):
    """Download documents for an issuer."""
    client = BMVClient()

    # Resolve ticker and ID
    if args.issuer_id:
        ticker = args.ticker
        issuer_id = args.issuer_id
    else:
        ticker, issuer_id = resolve_bmv_issuer(args.ticker)

    market = args.market
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading documents for {ticker}-{issuer_id} to {output_dir}")

    downloaded = client.download_all_documents(ticker, issuer_id, market, output_dir)

    print(f"\nDownloaded {len(downloaded)} files to {output_dir}")


def cmd_export(args):
    """Export full issuer data (info + documents) to files."""
    client = BMVClient()

    # Resolve ticker and ID
    if args.issuer_id:
        ticker = args.ticker
        issuer_id = args.issuer_id
    else:
        ticker, issuer_id = resolve_bmv_issuer(args.ticker)

    market = args.market
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Exporting data for {ticker}-{issuer_id}...")

    # Fetch all data
    issuer = client.get_issuer(ticker, issuer_id)
    securities = client.get_issuer_securities(ticker, issuer_id)
    documents = client.get_all_documents(ticker, issuer_id, market)

    # Save issuer info
    issuer_data = {
        "issuer": issuer.model_dump(),
        "securities": [s.model_dump() for s in securities],
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
            "category": doc.category,
            "doc_type": doc.doc_type,
            "period": doc.period,
            "filename": doc.filename,
            "is_xbrl": doc.is_xbrl,
            "download_url": doc.download_url,
        })

    df = pd.DataFrame(doc_records)
    docs_file = output_dir / "documents.csv"
    df.to_csv(docs_file, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} document records to {docs_file}")

    # Download if requested
    if args.download:
        pdfs_dir = output_dir / "files"
        print(f"\nDownloading {len(documents)} documents...")

        downloaded = client.download_all_documents(ticker, issuer_id, market, pdfs_dir)
        print(f"Downloaded {len(downloaded)} files")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="bmv",
        description="BMV (Bolsa Mexicana de Valores) data client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get issuer info
  bmv issuer LOCKXPI 35563
  bmv issuer LOCKXPI 35563 --securities

  # List documents
  bmv documents LOCKXPI 35563 CGEN_CAPIT
  bmv documents LOCKXPI 35563 CGEN_CAPIT --category financial
  bmv documents LOCKXPI 35563 CGEN_CAPIT --category events --output docs.csv

  # Download all documents
  bmv download LOCKXPI 35563 CGEN_CAPIT --output ./downloads

  # Full export (info + documents + files)
  bmv export LOCKXPI 35563 CGEN_CAPIT --output ./lockxpi --download

Market types:
  CGEN_CAPIT - Capitales (Equities, CKDs)
  CGEN_ELDEU - Deuda (Debt)
  CGEN_GLOB  - Global
  CGEN_CANC  - Cancelled
        """,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # issuer command
    issuer_parser = subparsers.add_parser("issuer", help="Get issuer information")
    issuer_parser.add_argument("ticker", help="Ticker symbol (e.g., LOCKXPI)")
    issuer_parser.add_argument(
        "issuer_id", nargs="?", type=int, help="Issuer ID (optional if known)"
    )
    issuer_parser.add_argument(
        "--securities", action="store_true", help="Include securities"
    )
    issuer_parser.add_argument("--json", action="store_true", help="Output as JSON")
    issuer_parser.set_defaults(func=cmd_issuer)

    # documents command
    docs_parser = subparsers.add_parser("documents", help="List issuer documents")
    docs_parser.add_argument("ticker", help="Ticker symbol")
    docs_parser.add_argument("issuer_id", nargs="?", type=int, help="Issuer ID")
    docs_parser.add_argument("market", help="Market type (e.g., CGEN_CAPIT)")
    docs_parser.add_argument("--output", "-o", help="Output CSV file")
    docs_parser.add_argument(
        "--category",
        choices=["all", "financial", "events", "corporate"],
        default="all",
        help="Document category filter",
    )
    docs_parser.add_argument("--json", action="store_true", help="Output as JSON")
    docs_parser.set_defaults(func=cmd_documents)

    # download command
    dl_parser = subparsers.add_parser("download", help="Download documents")
    dl_parser.add_argument("ticker", help="Ticker symbol")
    dl_parser.add_argument("issuer_id", nargs="?", type=int, help="Issuer ID")
    dl_parser.add_argument("market", help="Market type")
    dl_parser.add_argument("--output", "-o", required=True, help="Output directory")
    dl_parser.set_defaults(func=cmd_download)

    # export command
    export_parser = subparsers.add_parser("export", help="Export full issuer data")
    export_parser.add_argument("ticker", help="Ticker symbol")
    export_parser.add_argument("issuer_id", nargs="?", type=int, help="Issuer ID")
    export_parser.add_argument("market", help="Market type")
    export_parser.add_argument("--output", "-o", required=True, help="Output directory")
    export_parser.add_argument(
        "--download", "-d", action="store_true", help="Also download files"
    )
    export_parser.set_defaults(func=cmd_export)

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        args.func(args)
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        logger.error("Install with: pip install mx-exchange-dataclient[scraper]")
        sys.exit(1)
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
