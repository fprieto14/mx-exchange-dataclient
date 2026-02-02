"""Download manager with rate limiting and retry logic."""

import hashlib
import logging
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _sanitize_url_for_logging(url: str) -> str:
    """Remove sensitive query parameters from URL for safe logging.

    Args:
        url: Full URL

    Returns:
        URL with query parameters removed or masked
    """
    try:
        parsed = urlparse(url)
        # Keep only scheme, netloc, and path for logging
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        return "<invalid-url>"


class DownloadManager:
    """Manages document downloads with rate limiting, retries, and progress tracking.

    Features:
    - Configurable rate limiting between requests
    - Automatic retry on failures with exponential backoff
    - Progress callbacks for UI integration
    - Skip already-downloaded files
    - Checksum computation during download
    - Connection pooling for better performance
    """

    def __init__(
        self,
        session: requests.Session | None = None,
        rate_limit_delay: float = 0.3,
        max_retries: int = 3,
        timeout: int = 60,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
    ):
        """Initialize download manager.

        Args:
            session: Optional requests session (creates one if not provided)
            rate_limit_delay: Delay between downloads in seconds
            max_retries: Maximum retry attempts for failed downloads
            timeout: Request timeout in seconds
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections per pool
        """
        self.session = session or self._create_session(pool_connections, pool_maxsize)
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self._owns_session = session is None

        # Default headers if session doesn't have them
        if "User-Agent" not in self.session.headers:
            self.session.headers.update({
                "User-Agent": "mx-exchange-dataclient/0.1.0",
                "Accept": "*/*",
            })

    @staticmethod
    def _create_session(pool_connections: int, pool_maxsize: int) -> requests.Session:
        """Create a session with connection pooling.

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
            max_retries=Retry(total=0),  # We handle retries ourselves
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def close(self):
        """Close the session if we own it."""
        if self._owns_session and self.session:
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def download(
        self,
        url: str,
        filepath: Path,
        skip_existing: bool = True,
        delay: float | None = None,
        compute_checksum: bool = False,
    ) -> tuple[bool, str | None]:
        """Download a file from URL.

        Args:
            url: Source URL
            filepath: Destination file path
            skip_existing: Skip if file already exists
            delay: Override rate limit delay for this download
            compute_checksum: Whether to compute SHA256 checksum during download

        Returns:
            Tuple of (success, checksum). Checksum is None if not computed or failed.
        """
        safe_url = _sanitize_url_for_logging(url)

        if skip_existing and filepath.exists():
            logger.debug(f"Skipping existing file: {filepath.name}")
            # Compute checksum of existing file if requested
            if compute_checksum:
                checksum = self._compute_file_checksum(filepath)
                return True, checksum
            return True, None

        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Apply rate limiting
        actual_delay = delay if delay is not None else self.rate_limit_delay
        if actual_delay > 0:
            time.sleep(actual_delay)

        # Download with retries
        last_error = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout, stream=True)
                response.raise_for_status()

                # Write to file and optionally compute checksum
                sha256 = hashlib.sha256() if compute_checksum else None
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        if sha256:
                            sha256.update(chunk)

                checksum = f"sha256:{sha256.hexdigest()}" if sha256 else None
                logger.info(f"Downloaded: {filepath.name}")
                return True, checksum

            except requests.RequestException as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = (2**attempt) * 0.5  # Exponential backoff
                    logger.warning(
                        f"Download failed (attempt {attempt + 1}/{self.max_retries}): "
                        f"{type(e).__name__}. Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)

        logger.error(
            f"Failed to download {safe_url} after {self.max_retries} attempts: "
            f"{type(last_error).__name__}"
        )
        return False, None

    @staticmethod
    def _compute_file_checksum(filepath: Path) -> str:
        """Compute SHA256 checksum of an existing file.

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

    def download_batch(
        self,
        downloads: list[tuple[str, Path]],
        skip_existing: bool = True,
        progress_callback: Callable[[int, int, str], None] | None = None,
        compute_checksums: bool = False,
    ) -> tuple[list[Path], list[str], dict[str, str]]:
        """Download multiple files.

        Args:
            downloads: List of (url, filepath) tuples
            skip_existing: Skip if files already exist
            progress_callback: Called with (current, total, filename) for each file
            compute_checksums: Whether to compute checksums during download

        Returns:
            Tuple of (successful_paths, failed_urls, checksums)
            checksums is a dict of filename -> checksum
        """
        successful = []
        failed = []
        checksums = {}
        total = len(downloads)

        for i, (url, filepath) in enumerate(downloads):
            if progress_callback:
                progress_callback(i + 1, total, filepath.name)

            success, checksum = self.download(
                url, filepath, skip_existing=skip_existing, compute_checksum=compute_checksums
            )
            if success:
                successful.append(filepath)
                if checksum:
                    checksums[filepath.name] = checksum
            else:
                failed.append(_sanitize_url_for_logging(url))

        return successful, failed, checksums

    def download_with_checksum(
        self,
        url: str,
        filepath: Path,
        expected_checksum: str | None = None,
    ) -> tuple[bool, str | None]:
        """Download a file and verify/compute checksum.

        Args:
            url: Source URL
            filepath: Destination file path
            expected_checksum: Expected checksum to verify (sha256:...)

        Returns:
            Tuple of (success, computed_checksum)
            computed_checksum is None if download failed
        """
        success, computed = self.download(
            url, filepath, skip_existing=False, compute_checksum=True
        )

        if not success:
            return False, None

        # Verify if expected checksum provided
        if expected_checksum and computed != expected_checksum:
            logger.error(
                f"Checksum mismatch for {filepath.name}: "
                f"expected {expected_checksum}, got {computed}"
            )
            filepath.unlink()  # Remove corrupted file
            return False, computed

        return True, computed

    def set_rate_limit(self, delay: float):
        """Update the rate limit delay.

        Args:
            delay: New delay in seconds
        """
        self.rate_limit_delay = delay

    def set_session_header(self, key: str, value: str):
        """Set a session header.

        Args:
            key: Header name
            value: Header value
        """
        self.session.headers[key] = value
