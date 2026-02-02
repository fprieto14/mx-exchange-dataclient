"""Sync state persistence for tracking document synchronization."""

import hashlib
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator


class SyncState:
    """JSON-based state tracking for incremental sync.

    Tracks last sync time, document counts, and file checksums to enable
    incremental updates without re-downloading unchanged files.

    State file format:
    {
        "CAPGLPI": {
            "last_sync": "2026-02-01T15:30:00Z",
            "last_document_date": "2025-12-31",
            "document_count": 156,
            "file_checksums": {"file.xbrl": "sha256:..."}
        }
    }
    """

    def __init__(self, state_file: str | Path = ".sync_state.json"):
        """Initialize sync state.

        Args:
            state_file: Path to state file (relative to output_dir or absolute)
        """
        self.state_file = Path(state_file)
        self._state: dict[str, dict[str, Any]] = {}
        self._batch_mode = False
        self._load()

    @contextmanager
    def batch_updates(self) -> Iterator[None]:
        """Context manager for batching multiple state updates.

        Use this when making multiple updates to avoid writing the state file
        after each change. The state file will be written once when exiting
        the context.

        Example:
            with state.batch_updates():
                state.add_file_checksum(ticker, "file1.xbrl", checksum1)
                state.add_file_checksum(ticker, "file2.xbrl", checksum2)
                # State file written once here
        """
        self._batch_mode = True
        try:
            yield
        finally:
            self._batch_mode = False
            self._save()

    def _load(self):
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self._state = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._state = {}
        else:
            self._state = {}

    def _save(self, force: bool = False):
        """Save state to file.

        Args:
            force: Save even if in batch mode
        """
        if self._batch_mode and not force:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(self._state, f, indent=2, default=str)

    def get_issuer_state(self, ticker: str) -> dict[str, Any]:
        """Get state for a specific issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            State dict with keys: last_sync, last_document_date, document_count, file_checksums
        """
        return self._state.get(ticker, {})

    def get_last_sync(self, ticker: str) -> datetime | None:
        """Get last sync timestamp for an issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            Last sync datetime or None if never synced
        """
        state = self.get_issuer_state(ticker)
        if "last_sync" in state:
            return datetime.fromisoformat(state["last_sync"])
        return None

    def get_last_document_date(self, ticker: str) -> str | None:
        """Get the most recent document date for an issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            Date string (YYYY-MM-DD) or None
        """
        state = self.get_issuer_state(ticker)
        return state.get("last_document_date")

    def get_document_count(self, ticker: str) -> int:
        """Get stored document count for an issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            Document count or 0 if not tracked
        """
        state = self.get_issuer_state(ticker)
        return state.get("document_count", 0)

    def get_file_checksum(self, ticker: str, filename: str) -> str | None:
        """Get stored checksum for a file.

        Args:
            ticker: Issuer ticker
            filename: File name

        Returns:
            Checksum string or None if not tracked
        """
        state = self.get_issuer_state(ticker)
        checksums = state.get("file_checksums", {})
        return checksums.get(filename)

    def update_issuer_state(
        self,
        ticker: str,
        last_sync: datetime | None = None,
        last_document_date: str | None = None,
        document_count: int | None = None,
        file_checksums: dict[str, str] | None = None,
    ):
        """Update state for an issuer.

        Args:
            ticker: Issuer ticker
            last_sync: Sync timestamp (default: now)
            last_document_date: Most recent document date
            document_count: Total document count
            file_checksums: Dict of filename -> checksum
        """
        if ticker not in self._state:
            self._state[ticker] = {}

        state = self._state[ticker]

        if last_sync is not None:
            state["last_sync"] = last_sync.isoformat()
        elif "last_sync" not in state:
            state["last_sync"] = datetime.now().isoformat()

        if last_document_date is not None:
            state["last_document_date"] = last_document_date

        if document_count is not None:
            state["document_count"] = document_count

        if file_checksums is not None:
            if "file_checksums" not in state:
                state["file_checksums"] = {}
            state["file_checksums"].update(file_checksums)

        self._save()

    def add_file_checksum(self, ticker: str, filename: str, checksum: str, save: bool = True):
        """Add or update checksum for a single file.

        Args:
            ticker: Issuer ticker
            filename: File name
            checksum: Checksum string
            save: Whether to save immediately (ignored in batch mode)
        """
        if ticker not in self._state:
            self._state[ticker] = {}

        if "file_checksums" not in self._state[ticker]:
            self._state[ticker]["file_checksums"] = {}

        self._state[ticker]["file_checksums"][filename] = checksum
        if save:
            self._save()

    def remove_issuer(self, ticker: str):
        """Remove all state for an issuer.

        Args:
            ticker: Issuer ticker
        """
        if ticker in self._state:
            del self._state[ticker]
            self._save()

    def clear(self):
        """Clear all state."""
        self._state = {}
        self._save()

    @staticmethod
    def compute_file_checksum(filepath: Path) -> str:
        """Compute SHA256 checksum for a file.

        Args:
            filepath: Path to file

        Returns:
            Checksum string prefixed with 'sha256:'
        """
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return f"sha256:{sha256.hexdigest()}"

    def is_file_changed(self, ticker: str, filepath: Path) -> bool:
        """Check if a file has changed since last sync.

        Args:
            ticker: Issuer ticker
            filepath: Path to file

        Returns:
            True if file is new or changed, False if unchanged
        """
        stored_checksum = self.get_file_checksum(ticker, filepath.name)
        if stored_checksum is None:
            return True

        current_checksum = self.compute_file_checksum(filepath)
        return current_checksum != stored_checksum

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """Get full state as dict.

        Returns:
            Complete state dictionary
        """
        return dict(self._state)
