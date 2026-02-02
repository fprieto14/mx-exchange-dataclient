"""
Unified CLI for MX-Exchange-dataclient

Command-line interface for syncing and analyzing Mexican exchange data.

Usage:
    mxdata sync CAPGLPI                    # Incremental sync
    mxdata sync CAPGLPI --full             # Full sync
    mxdata sync CAPGLPI --xbrl-only        # Only XBRL
    mxdata issuer CAPGLPI                  # Show info
    mxdata xbrl reconcile ./data/CAPGLPI   # NAV reconciliation
"""

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def cmd_sync(args):
    """Synchronize documents for an issuer."""
    from mx_exchange_dataclient.sync import SyncEngine

    engine = SyncEngine(
        output_dir=args.output or "./data",
        rate_limit_delay=args.delay,
    )

    def progress(current, total, filename):
        if current % 10 == 0 or current == total:
            pct = current / total * 100 if total > 0 else 0
            print(f"  [{current}/{total}] {pct:.1f}% - {filename}")

    if args.xbrl_only:
        stats = engine.sync_xbrl_only(
            args.ticker,
            source=args.source,
            progress_callback=progress,
            dry_run=args.dry_run,
        )
    else:
        mode = "full" if args.full else "incremental"
        stats = engine.sync(
            args.ticker,
            mode=mode,
            source=args.source,
            progress_callback=progress,
            dry_run=args.dry_run,
        )

    # Print summary
    print(f"\n{'=' * 50}")
    print(f"SYNC COMPLETE: {args.ticker}")
    print(f"{'=' * 50}")
    print(f"Source:     {stats.get('source', 'unknown')}")
    print(f"Mode:       {stats.get('mode', 'unknown')}")
    print(f"Documents found:      {stats.get('documents_found', 0)}")
    print(f"Documents new:        {stats.get('documents_new', 0)}")
    print(f"Documents downloaded: {stats.get('documents_downloaded', 0)}")
    print(f"Documents skipped:    {stats.get('documents_skipped', 0)}")
    print(f"Documents failed:     {stats.get('documents_failed', 0)}")

    if stats.get("error"):
        print(f"Error: {stats['error']}")


def cmd_status(args):
    """Show sync status for an issuer."""
    from mx_exchange_dataclient.sync import SyncEngine

    engine = SyncEngine(output_dir=args.output or "./data")

    status = engine.get_sync_status(args.ticker)

    print(f"\n{'=' * 50}")
    print(f"STATUS: {args.ticker}")
    print(f"{'=' * 50}")
    print(f"Last sync:           {status.get('last_sync', 'Never')}")
    print(f"Last document date:  {status.get('last_document_date', 'N/A')}")
    print(f"Stored document count: {status.get('stored_document_count', 0)}")
    print(f"Local file count:    {status.get('local_file_count', 0)}")

    by_type = status.get("local_files_by_type", {})
    if by_type:
        print("\nFiles by type:")
        for doc_type, count in sorted(by_type.items()):
            print(f"  {doc_type}: {count}")


def cmd_issuer(args):
    """Show issuer information."""
    from mx_exchange_dataclient.data import get_issuer_info

    info = get_issuer_info(args.ticker)

    if not info:
        # Try to fetch from API
        source = args.source or "biva"
        if source == "biva":
            from mx_exchange_dataclient.clients.biva import BIVAClient

            client = BIVAClient()
            try:
                issuer = client.get_issuer(args.ticker)
                print(f"\n{'=' * 50}")
                print(f"ISSUER: {issuer.clave}")
                print(f"{'=' * 50}")
                print(f"ID:           {issuer.id}")
                print(f"Razón Social: {issuer.razon_social}")
                print(f"Source:       BIVA")
                print(f"Status:       {issuer.estatus}")
                return
            except Exception as e:
                print(f"Error fetching issuer: {e}")
                sys.exit(1)
        else:
            print(f"Unknown issuer: {args.ticker}")
            sys.exit(1)

    print(f"\n{'=' * 50}")
    print(f"ISSUER: {args.ticker}")
    print(f"{'=' * 50}")
    print(f"ID:      {info.get('id')}")
    print(f"Name:    {info.get('name', 'N/A')}")
    print(f"Source:  {info.get('source', 'unknown')}")
    if info.get("market"):
        print(f"Market:  {info.get('market')}")


def cmd_xbrl_reconcile(args):
    """Run NAV reconciliation on XBRL files."""
    from mx_exchange_dataclient.xbrl import nav_reconciliation_by_period, ReportPeriod

    xbrl_folder = Path(args.folder)
    if not xbrl_folder.exists():
        print(f"Folder not found: {xbrl_folder}")
        sys.exit(1)

    # Determine ticker from folder name
    ticker = args.ticker or xbrl_folder.name

    try:
        period_type = ReportPeriod(args.period.lower())
    except ValueError:
        print(f"Invalid period type: {args.period}")
        print("Valid options: quarterly, ytd, ltm, annual, itd")
        sys.exit(1)

    try:
        report = nav_reconciliation_by_period(
            ticker=ticker,
            xbrl_folder=str(xbrl_folder),
            period_type=period_type,
            as_of=args.as_of,
        )
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        report.print_report()

    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2, default=str)
        print(f"\nSaved report to {output_path}")


def cmd_organize(args):
    """Organize downloaded files by document type."""
    from mx_exchange_dataclient.utils.file_organizer import organize_output_folder

    output_dir = Path(args.folder)
    if not output_dir.exists():
        print(f"Folder not found: {output_dir}")
        sys.exit(1)

    stats = organize_output_folder(output_dir, dry_run=args.dry_run)

    print(f"\n{'=' * 50}")
    print("ORGANIZATION COMPLETE")
    print(f"{'=' * 50}")
    print(f"Files moved: {stats['moved']}")
    print(f"Files skipped: {stats['skipped']}")
    if stats["errors"]:
        print(f"Errors: {len(stats['errors'])}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="mxdata",
        description="Mexican Exchange Data Client - sync and analyze BIVA/BMV data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync documents
  mxdata sync CAPGLPI                    # Incremental sync
  mxdata sync CAPGLPI --full             # Full sync
  mxdata sync CAPGLPI --xbrl-only        # Only XBRL files

  # Check status
  mxdata status CAPGLPI

  # Show issuer info
  mxdata issuer CAPGLPI

  # NAV reconciliation
  mxdata xbrl reconcile ./data/CAPGLPI/ReporteTrimestral
  mxdata xbrl reconcile ./data/CAPGLPI/ReporteTrimestral --period ltm

  # Organize files
  mxdata organize ./data

Use 'mxdata <command> --help' for more information on each command.
        """,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # sync command
    sync_parser = subparsers.add_parser("sync", help="Synchronize documents")
    sync_parser.add_argument("ticker", help="Issuer ticker (e.g., CAPGLPI)")
    sync_parser.add_argument("--output", "-o", help="Output directory (default: ./data)")
    sync_parser.add_argument(
        "--full", action="store_true", help="Full sync (re-download all)"
    )
    sync_parser.add_argument(
        "--xbrl-only", action="store_true", help="Only sync XBRL files"
    )
    sync_parser.add_argument(
        "--source",
        choices=["biva", "bmv"],
        help="Force source (auto-detected by default)",
    )
    sync_parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Delay between downloads in seconds (default: 0.3)",
    )
    sync_parser.add_argument(
        "--dry-run", action="store_true", help="Don't download, just show what would be done"
    )
    sync_parser.set_defaults(func=cmd_sync)

    # status command
    status_parser = subparsers.add_parser("status", help="Show sync status")
    status_parser.add_argument("ticker", help="Issuer ticker")
    status_parser.add_argument("--output", "-o", help="Data directory (default: ./data)")
    status_parser.set_defaults(func=cmd_status)

    # issuer command
    issuer_parser = subparsers.add_parser("issuer", help="Show issuer information")
    issuer_parser.add_argument("ticker", help="Issuer ticker")
    issuer_parser.add_argument(
        "--source", choices=["biva", "bmv"], help="Force source for API lookup"
    )
    issuer_parser.set_defaults(func=cmd_issuer)

    # xbrl subcommand group
    xbrl_parser = subparsers.add_parser("xbrl", help="XBRL analysis commands")
    xbrl_subparsers = xbrl_parser.add_subparsers(dest="xbrl_command")

    # xbrl reconcile
    recon_parser = xbrl_subparsers.add_parser(
        "reconcile", help="Run NAV reconciliation"
    )
    recon_parser.add_argument("folder", help="Folder containing XBRL files")
    recon_parser.add_argument("--ticker", help="Fund ticker (default: folder name)")
    recon_parser.add_argument(
        "--period",
        default="ltm",
        choices=["quarterly", "ytd", "ltm", "annual", "itd"],
        help="Analysis period type (default: ltm)",
    )
    recon_parser.add_argument(
        "--as-of", help="Reference date (YYYY-MM-DD or YYYY-QN format)"
    )
    recon_parser.add_argument("--output", "-o", help="Save report to JSON file")
    recon_parser.add_argument("--json", action="store_true", help="Output as JSON")
    recon_parser.set_defaults(func=cmd_xbrl_reconcile)

    # organize command
    org_parser = subparsers.add_parser(
        "organize", help="Organize downloaded files by type"
    )
    org_parser.add_argument(
        "folder", help="Folder to organize (default: ./data)", nargs="?", default="./data"
    )
    org_parser.add_argument(
        "--dry-run", action="store_true", help="Don't move, just show what would be done"
    )
    org_parser.set_defaults(func=cmd_organize)

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Handle xbrl subcommand
    if args.command == "xbrl" and not hasattr(args, "func"):
        xbrl_parser.print_help()
        sys.exit(1)

    try:
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
