"""SyncEngine - orchestrates bulk and incremental document synchronization."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mx_exchange_dataclient.clients.biva import BIVAClient
from mx_exchange_dataclient.data import get_issuer_info
from mx_exchange_dataclient.sync.download import DownloadManager
from mx_exchange_dataclient.sync.state import SyncState
from mx_exchange_dataclient.sync.storage import StorageLayout

logger = logging.getLogger(__name__)


class SyncEngine:
    """Orchestrates document synchronization for Mexican exchange issuers.

    Supports both BIVA and BMV sources with:
    - Full sync: Download all documents
    - Incremental sync: Only new documents since last sync
    - XBRL-only sync: Only quarterly XBRL financial statements

    Example:
        engine = SyncEngine(output_dir="./data")
        engine.sync("CAPGLPI", mode="incremental")  # Only new docs
        engine.sync("CAPGLPI", mode="full")          # Re-download all
        engine.sync_xbrl_only("CAPGLPI")             # Just quarterly XBRL

        # Use as context manager for automatic cleanup
        with SyncEngine(output_dir="./data") as engine:
            engine.sync("CAPGLPI")
    """

    def __init__(
        self,
        output_dir: str | Path = "./data",
        rate_limit_delay: float = 0.3,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
    ):
        """Initialize sync engine.

        Args:
            output_dir: Root directory for downloaded documents
            rate_limit_delay: Delay between downloads in seconds
            pool_connections: Number of connection pools
            pool_maxsize: Max connections per pool
        """
        self.output_dir = Path(output_dir)
        self.storage = StorageLayout(output_dir)
        self.state = SyncState(self.output_dir / ".sync_state.json")
        self._rate_limit_delay = rate_limit_delay

        # Create shared session for all clients
        self._session = self._create_shared_session(pool_connections, pool_maxsize)
        self.download_manager = DownloadManager(
            session=self._session,
            rate_limit_delay=rate_limit_delay,
        )

        # Lazy-loaded clients
        self._biva_client: BIVAClient | None = None
        self._bmv_client = None  # Type depends on availability

    @staticmethod
    def _create_shared_session(pool_connections: int, pool_maxsize: int) -> requests.Session:
        """Create a shared session with connection pooling.

        Args:
            pool_connections: Number of connection pools
            pool_maxsize: Max connections per pool

        Returns:
            Configured requests session
        """
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=Retry(total=0),
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def close(self):
        """Close the shared session and cleanup resources."""
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def biva_client(self) -> BIVAClient:
        """Get or create BIVA client (shares session with engine)."""
        if self._biva_client is None:
            self._biva_client = BIVAClient(
                rate_limit_delay=self._rate_limit_delay,
                session=self._session,
            )
        return self._biva_client

    @property
    def bmv_client(self):
        """Get or create BMV client (shares session, raises ImportError if not available)."""
        if self._bmv_client is None:
            from mx_exchange_dataclient.clients.bmv import BMVClient

            self._bmv_client = BMVClient(
                rate_limit_delay=self._rate_limit_delay,
                session=self._session,
            )
        return self._bmv_client

    def sync(
        self,
        ticker: str,
        mode: str = "incremental",
        source: str | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        dry_run: bool = False,
    ) -> dict:
        """Synchronize documents for an issuer.

        Args:
            ticker: Issuer ticker (e.g., "CAPGLPI", "LOCKXPI")
            mode: "full" or "incremental"
            source: "biva", "bmv", or None (auto-detect)
            progress_callback: Called with (current, total, filename)
            dry_run: If True, don't download, just report what would be done

        Returns:
            Dict with sync statistics
        """
        stats = {
            "ticker": ticker,
            "mode": mode,
            "source": source,
            "documents_found": 0,
            "documents_new": 0,
            "documents_downloaded": 0,
            "documents_skipped": 0,
            "documents_failed": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
        }

        # Auto-detect source if not specified
        if source is None:
            source = self._detect_source(ticker)
        stats["source"] = source

        logger.info(f"Starting {mode} sync for {ticker} from {source}")

        try:
            if source == "biva":
                stats = self._sync_biva(ticker, mode, progress_callback, dry_run, stats)
            elif source == "bmv":
                stats = self._sync_bmv(ticker, mode, progress_callback, dry_run, stats)
            else:
                raise ValueError(f"Unknown source: {source}")

        except Exception as e:
            logger.error(f"Sync failed for {ticker}: {e}")
            stats["error"] = str(e)

        stats["end_time"] = datetime.now().isoformat()
        return stats

    def _detect_source(self, ticker: str) -> str:
        """Detect the exchange source for a ticker.

        Args:
            ticker: Issuer ticker

        Returns:
            "biva" or "bmv"
        """
        # Try to get issuer info from data module
        info = get_issuer_info(ticker)
        if info:
            return info.get("source", "biva")

        # Default to BIVA for now
        return "biva"

    def _sync_biva(
        self,
        ticker: str,
        mode: str,
        progress_callback: Callable[[int, int, str], None] | None,
        dry_run: bool,
        stats: dict,
    ) -> dict:
        """Sync documents from BIVA.

        Args:
            ticker: Issuer ticker
            mode: "full" or "incremental"
            progress_callback: Progress callback
            dry_run: Don't actually download
            stats: Stats dict to update

        Returns:
            Updated stats dict
        """
        client = self.biva_client

        # Get document list
        documents = list(client.iter_documents(ticker))
        stats["documents_found"] = len(documents)

        # Filter for incremental sync
        if mode == "incremental":
            last_sync = self.state.get_last_sync(ticker)
            if last_sync:
                documents = [
                    d
                    for d in documents
                    if d.fecha_publicacion is None or d.fecha_publicacion > last_sync
                ]
        stats["documents_new"] = len(documents)

        if dry_run:
            for doc in documents:
                logger.info(f"Would download: {doc.file_name}")
            return stats

        # Download documents with batched state updates
        total = len(documents)
        with self.state.batch_updates():
            for i, doc in enumerate(documents):
                if progress_callback:
                    progress_callback(i + 1, total, doc.file_name)

                # Determine target path
                date_str = None
                if doc.fecha_publicacion:
                    date_str = doc.fecha_publicacion.strftime("%Y%m%d")

                target_path = self.storage.get_target_path(ticker, doc.file_name, date_str)

                # Download with checksum computation
                success, checksum = self.download_manager.download(
                    doc.download_url, target_path, skip_existing=True, compute_checksum=True
                )

                if success:
                    if target_path.exists():
                        stats["documents_downloaded"] += 1
                        # Update checksum (will be saved when batch exits)
                        if checksum:
                            self.state.add_file_checksum(ticker, target_path.name, checksum)
                    else:
                        stats["documents_skipped"] += 1
                else:
                    stats["documents_failed"] += 1

            # Update issuer state (also batched)
            last_doc_date = None
            if documents:
                dates = [d.fecha_publicacion for d in documents if d.fecha_publicacion]
                if dates:
                    last_doc_date = max(dates).strftime("%Y-%m-%d")

            self.state.update_issuer_state(
                ticker,
                last_sync=datetime.now(),
                last_document_date=last_doc_date,
                document_count=stats["documents_found"],
            )

        return stats

    def _sync_bmv(
        self,
        ticker: str,
        mode: str,
        progress_callback: Callable[[int, int, str], None] | None,
        dry_run: bool,
        stats: dict,
    ) -> dict:
        """Sync documents from BMV.

        Args:
            ticker: Issuer ticker (may be "TICKER-ID-MARKET" format)
            mode: "full" or "incremental"
            progress_callback: Progress callback
            dry_run: Don't actually download
            stats: Stats dict to update

        Returns:
            Updated stats dict
        """
        client = self.bmv_client

        # Parse ticker format
        parts = ticker.upper().split("-")
        if len(parts) >= 3:
            bmv_ticker = parts[0]
            issuer_id = int(parts[1])
            market = parts[2]
        elif len(parts) == 2:
            bmv_ticker = parts[0]
            issuer_id = int(parts[1])
            market = "CGEN_CAPIT"
        else:
            from mx_exchange_dataclient.models.bmv import resolve_bmv_issuer

            bmv_ticker, issuer_id = resolve_bmv_issuer(ticker)
            market = "CGEN_CAPIT"

        # Get document list
        documents = list(client.iter_all_documents(bmv_ticker, issuer_id, market))
        stats["documents_found"] = len(documents)
        stats["documents_new"] = len(documents)  # BMV doesn't have reliable dates

        if dry_run:
            for doc in documents:
                logger.info(f"Would download: {doc.filename}")
            return stats

        # Download documents with batched state updates
        total = len(documents)
        with self.state.batch_updates():
            for i, doc in enumerate(documents):
                if progress_callback:
                    progress_callback(i + 1, total, doc.filename)

                target_path = self.storage.get_target_path(bmv_ticker, doc.filename)

                # Download with checksum computation
                success, checksum = self.download_manager.download(
                    doc.download_url, target_path, skip_existing=True, compute_checksum=True
                )

                if success:
                    if target_path.exists():
                        stats["documents_downloaded"] += 1
                        if checksum:
                            self.state.add_file_checksum(bmv_ticker, target_path.name, checksum)
                    else:
                        stats["documents_skipped"] += 1
                else:
                    stats["documents_failed"] += 1

            # Update state (batched)
            self.state.update_issuer_state(
                bmv_ticker,
                last_sync=datetime.now(),
                document_count=stats["documents_found"],
            )

        return stats

    def sync_xbrl_only(
        self,
        ticker: str,
        source: str | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        dry_run: bool = False,
    ) -> dict:
        """Synchronize only XBRL financial statements.

        Args:
            ticker: Issuer ticker
            source: "biva", "bmv", or None (auto-detect)
            progress_callback: Called with (current, total, filename)
            dry_run: If True, don't download

        Returns:
            Dict with sync statistics
        """
        stats = {
            "ticker": ticker,
            "mode": "xbrl_only",
            "source": source or self._detect_source(ticker),
            "documents_found": 0,
            "documents_downloaded": 0,
            "documents_skipped": 0,
            "documents_failed": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
        }

        source = stats["source"]
        logger.info(f"Starting XBRL-only sync for {ticker} from {source}")

        try:
            if source == "biva":
                # Filter for XBRL documents
                documents = [
                    d
                    for d in self.biva_client.iter_documents(ticker)
                    if d.doc_type.lower() in ("xbrl", "zip")
                    or "trimestral" in d.tipo_documento.lower()
                ]
                stats["documents_found"] = len(documents)

                if dry_run:
                    for doc in documents:
                        logger.info(f"Would download: {doc.file_name}")
                else:
                    total = len(documents)
                    with self.state.batch_updates():
                        for i, doc in enumerate(documents):
                            if progress_callback:
                                progress_callback(i + 1, total, doc.file_name)

                            # Get XBRL URL if available
                            url = doc.xbrl_url or doc.download_url
                            date_str = (
                                doc.fecha_publicacion.strftime("%Y%m%d")
                                if doc.fecha_publicacion
                                else None
                            )
                            target_path = self.storage.get_target_path(
                                ticker, doc.file_name, date_str
                            )

                            success, checksum = self.download_manager.download(
                                url, target_path, skip_existing=True, compute_checksum=True
                            )
                            if success:
                                stats["documents_downloaded"] += 1
                                if checksum:
                                    self.state.add_file_checksum(ticker, target_path.name, checksum)
                            else:
                                stats["documents_failed"] += 1

            elif source == "bmv":
                # Filter for XBRL documents
                from mx_exchange_dataclient.models.bmv import resolve_bmv_issuer

                bmv_ticker, issuer_id = resolve_bmv_issuer(ticker)
                documents = [
                    d
                    for d in self.bmv_client.get_financial_documents(
                        bmv_ticker, issuer_id, "CGEN_CAPIT"
                    )
                    if d.is_xbrl
                ]
                stats["documents_found"] = len(documents)

                if dry_run:
                    for doc in documents:
                        logger.info(f"Would download: {doc.filename}")
                else:
                    total = len(documents)
                    with self.state.batch_updates():
                        for i, doc in enumerate(documents):
                            if progress_callback:
                                progress_callback(i + 1, total, doc.filename)

                            target_path = self.storage.get_target_path(bmv_ticker, doc.filename)
                            success, checksum = self.download_manager.download(
                                doc.download_url, target_path, skip_existing=True, compute_checksum=True
                            )
                            if success:
                                stats["documents_downloaded"] += 1
                                if checksum:
                                    self.state.add_file_checksum(bmv_ticker, target_path.name, checksum)
                            else:
                                stats["documents_failed"] += 1

        except Exception as e:
            logger.error(f"XBRL sync failed for {ticker}: {e}")
            stats["error"] = str(e)

        stats["end_time"] = datetime.now().isoformat()
        return stats

    def get_sync_status(self, ticker: str) -> dict:
        """Get current sync status for an issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            Dict with status info
        """
        state = self.state.get_issuer_state(ticker)
        documents = self.storage.list_documents(ticker)

        return {
            "ticker": ticker,
            "last_sync": state.get("last_sync"),
            "last_document_date": state.get("last_document_date"),
            "stored_document_count": state.get("document_count", 0),
            "local_file_count": len(documents),
            "local_files_by_type": self._count_by_type(ticker),
        }

    def _count_by_type(self, ticker: str) -> dict[str, int]:
        """Count local files by document type.

        Args:
            ticker: Issuer ticker

        Returns:
            Dict of doc_type -> count
        """
        issuer_dir = self.storage.get_issuer_dir(ticker)
        counts = {}

        for sub_dir in issuer_dir.iterdir():
            if sub_dir.is_dir():
                count = len([f for f in sub_dir.iterdir() if f.is_file()])
                if count > 0:
                    counts[sub_dir.name] = count

        return counts
