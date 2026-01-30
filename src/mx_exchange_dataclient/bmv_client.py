"""
BMV Client - HTML scraping client for Bolsa Mexicana de Valores.

Unlike BIVAClient which uses REST APIs, BMVClient scrapes HTML pages
to extract issuer and document information from bmv.com.mx.
"""

import logging
import re
import time
from pathlib import Path
from typing import Iterator

import requests

try:
    from bs4 import BeautifulSoup

    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

from biva_client.bmv_models import (
    BMVDocument,
    BMVIssuer,
    BMVSecurity,
    DOC_TYPE_CATEGORIES,
    resolve_bmv_issuer,
)

logger = logging.getLogger(__name__)


class BMVClient:
    """
    Client for BMV (Bolsa Mexicana de Valores) via HTML scraping.

    Unlike BIVAClient which uses REST APIs, BMVClient scrapes HTML pages
    to extract issuer and document information.

    Example:
        >>> client = BMVClient()
        >>> issuer = client.get_issuer("LOCKXPI", 35563)
        >>> print(issuer.name)

        >>> for doc in client.iter_all_documents("LOCKXPI", 35563, "CGEN_CAPIT"):
        ...     print(doc.filename, doc.download_url)

    Note:
        Requires beautifulsoup4 and lxml. Install with:
        pip install biva-client[scraper]
    """

    BASE_URL = "https://www.bmv.com.mx"

    def __init__(
        self,
        timeout: int = 30,
        rate_limit_delay: float = 0.5,
    ):
        """
        Initialize BMV client.

        Args:
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between requests (seconds)
        """
        if not HAS_BS4:
            raise ImportError(
                "beautifulsoup4 is required for BMVClient. "
                "Install with: pip install biva-client[scraper]"
            )

        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "bmv-client/0.1.0 (Mexican Exchange Data Client)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _fetch_page(self, path: str) -> "BeautifulSoup":
        """
        Fetch and parse an HTML page.

        Args:
            path: URL path (e.g., "/en/issuers/profile/LOCKXPI-35563")

        Returns:
            BeautifulSoup object
        """
        url = f"{self.BASE_URL}{path}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml")

    def _build_issuer_url(self, ticker: str, issuer_id: int, page_type: str = "profile") -> str:
        """Build URL for an issuer page."""
        slug = f"{ticker}-{issuer_id}"
        if page_type == "profile":
            return f"/en/issuers/profile/{slug}"
        elif page_type == "statistics":
            return f"/en/issuers/statistics/{slug}"
        else:
            raise ValueError(f"Unknown page type: {page_type}")

    def _build_info_url(
        self, ticker: str, issuer_id: int, market: str, info_type: str
    ) -> str:
        """Build URL for issuer information pages."""
        slug = f"{ticker}-{issuer_id}-{market}"
        return f"/en/issuers/{info_type}/{slug}"

    # -------------------------------------------------------------------------
    # Issuer Methods
    # -------------------------------------------------------------------------

    def get_issuer(self, ticker: str, issuer_id: int | None = None) -> BMVIssuer:
        """
        Get issuer information from profile page.

        Args:
            ticker: Ticker symbol (e.g., "LOCKXPI") or "TICKER-ID" format
            issuer_id: Issuer ID (optional if included in ticker or in KNOWN_BMV_ISSUERS)

        Returns:
            BMVIssuer object with profile details
        """
        if issuer_id is None:
            ticker, issuer_id = resolve_bmv_issuer(ticker)

        path = self._build_issuer_url(ticker, issuer_id, "profile")
        soup = self._fetch_page(path)

        # Parse issuer info from profile page
        name = ""
        status = "ACTIVA"
        market = "CGEN_CAPIT"

        # Try to extract name from the profile header
        header = soup.find("h1", class_="header-title")
        if header:
            name = header.get_text(strip=True)

        # Look for status in the page
        status_elem = soup.find(string=re.compile(r"Status|Estado", re.I))
        if status_elem:
            parent = status_elem.find_parent()
            if parent:
                status_text = parent.get_text(strip=True)
                if "CANCEL" in status_text.upper():
                    status = "CANCELADA"

        # Extract market type from page links or breadcrumb
        market_links = soup.find_all("a", href=re.compile(r"CGEN_"))
        for link in market_links:
            href = link.get("href", "")
            match = re.search(r"(CGEN_\w+)", href)
            if match:
                market = match.group(1)
                break

        return BMVIssuer(
            ticker=ticker,
            id=issuer_id,
            name=name or ticker,
            market=market,
            status=status,
        )

    def get_issuer_securities(
        self, ticker: str, issuer_id: int | None = None
    ) -> list[BMVSecurity]:
        """
        Get securities listed under an issuer.

        Args:
            ticker: Ticker symbol or "TICKER-ID" format
            issuer_id: Issuer ID (optional if included in ticker)

        Returns:
            List of BMVSecurity objects
        """
        if issuer_id is None:
            ticker, issuer_id = resolve_bmv_issuer(ticker)

        path = self._build_issuer_url(ticker, issuer_id, "profile")
        soup = self._fetch_page(path)

        securities = []

        # Look for securities table
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "serie" in headers or "series" in headers:
                rows = table.find_all("tr")[1:]  # Skip header row
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        securities.append(
                            BMVSecurity(
                                name=cells[0].get_text(strip=True),
                                series=cells[1].get_text(strip=True) if len(cells) > 1 else "",
                                status="ACTIVA",
                            )
                        )

        return securities

    # -------------------------------------------------------------------------
    # Document Methods
    # -------------------------------------------------------------------------

    def _parse_documents_table(
        self, soup: "BeautifulSoup", category: str
    ) -> list[BMVDocument]:
        """
        Parse documents from an HTML table.

        Args:
            soup: BeautifulSoup object of the page
            category: Document category (e.g., "Financial Statements")

        Returns:
            List of BMVDocument objects
        """
        documents = []

        # Find document links (PDFs and XBRL viewers)
        doc_links = soup.find_all("a", href=re.compile(r"docs-pub/|visorXbrl"))

        for link in doc_links:
            href = link.get("href", "")
            if not href:
                continue

            # Make absolute URL
            if href.startswith("/"):
                url = f"{self.BASE_URL}{href}"
            elif href.startswith("../"):
                url = f"{self.BASE_URL}/docs-pub/{href.replace('../', '')}"
            elif not href.startswith("http"):
                url = f"{self.BASE_URL}/{href}"
            else:
                url = href

            # Extract filename and doc type
            filename = ""
            doc_type = ""
            period = None
            is_xbrl = False

            if "visorXbrl" in url:
                is_xbrl = True
                # Extract ZIP filename from viewer URL
                match = re.search(r"docins=\.\./([\w/]+\.zip)", url)
                if match:
                    filename = match.group(1).split("/")[-1]
            else:
                filename = url.split("/")[-1]

            # Parse doc type from filename
            # Format: doctype_docid_seriesid_period_version.ext
            if "_" in filename:
                doc_type = filename.split("_")[0]

            # Extract period (e.g., 2025-03)
            period_match = re.search(r"_(\d{4}-\d{2})_", filename)
            if period_match:
                period = period_match.group(1)
            else:
                # Try annual period
                period_match = re.search(r"_(\d{4})_", filename)
                if period_match:
                    period = period_match.group(1)

            # Generate document ID
            doc_id = filename.replace(".", "_").replace("-", "_")

            # Get category from doc_type
            doc_category = DOC_TYPE_CATEGORIES.get(doc_type, category)

            documents.append(
                BMVDocument(
                    id=doc_id,
                    doc_type=doc_type,
                    category=doc_category,
                    filename=filename,
                    url=url,
                    period=period,
                    is_xbrl=is_xbrl,
                )
            )

        return documents

    def get_financial_documents(
        self, ticker: str, issuer_id: int, market: str
    ) -> list[BMVDocument]:
        """
        Get financial documents (XBRL statements, quarterly reports).

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type (e.g., "CGEN_CAPIT")

        Returns:
            List of BMVDocument objects
        """
        path = self._build_info_url(ticker, issuer_id, market, "financialinformation")
        soup = self._fetch_page(path)
        return self._parse_documents_table(soup, "Financial Statements")

    def get_relevant_events(
        self, ticker: str, issuer_id: int, market: str
    ) -> list[BMVDocument]:
        """
        Get relevant events documents.

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type (e.g., "CGEN_CAPIT")

        Returns:
            List of BMVDocument objects
        """
        path = self._build_info_url(ticker, issuer_id, market, "relevantevents")
        soup = self._fetch_page(path)
        return self._parse_documents_table(soup, "Relevant Events")

    def get_corporate_documents(
        self, ticker: str, issuer_id: int, market: str
    ) -> list[BMVDocument]:
        """
        Get corporate information documents.

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type (e.g., "CGEN_CAPIT")

        Returns:
            List of BMVDocument objects
        """
        path = self._build_info_url(ticker, issuer_id, market, "corporativeinformation")
        soup = self._fetch_page(path)
        return self._parse_documents_table(soup, "Corporate Information")

    def iter_all_documents(
        self, ticker: str, issuer_id: int, market: str
    ) -> Iterator[BMVDocument]:
        """
        Iterate over all documents from all categories.

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type (e.g., "CGEN_CAPIT")

        Yields:
            BMVDocument objects from all categories
        """
        # Fetch from each category with rate limiting
        for docs in [
            self.get_financial_documents(ticker, issuer_id, market),
        ]:
            yield from docs
            time.sleep(self.rate_limit_delay)

        for docs in [
            self.get_relevant_events(ticker, issuer_id, market),
        ]:
            yield from docs
            time.sleep(self.rate_limit_delay)

        for docs in [
            self.get_corporate_documents(ticker, issuer_id, market),
        ]:
            yield from docs

    def get_all_documents(
        self, ticker: str, issuer_id: int, market: str
    ) -> list[BMVDocument]:
        """
        Get all documents from all categories.

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type

        Returns:
            List of all BMVDocument objects
        """
        return list(self.iter_all_documents(ticker, issuer_id, market))

    # -------------------------------------------------------------------------
    # Download Methods
    # -------------------------------------------------------------------------

    def download_document(
        self,
        document: BMVDocument,
        output_dir: str | Path,
        delay: float = 0.3,
    ) -> Path | None:
        """
        Download a document file.

        Args:
            document: BMVDocument object to output
            output_dir: Directory to save file
            delay: Delay before output (rate limiting)

        Returns:
            Path to downloaded file, or None if failed
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        url = document.download_url

        # Generate filename with category prefix
        safe_category = "".join(
            c for c in document.category if c.isalnum() or c in " _-"
        )[:30]
        filename = f"{safe_category}_{document.filename}"
        filepath = output_dir / filename

        # Skip if exists
        if filepath.exists():
            logger.debug(f"Already exists: {filename}")
            return filepath

        try:
            time.sleep(delay)
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded: {filename}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to output {url}: {e}")
            return None

    def download_all_documents(
        self,
        ticker: str,
        issuer_id: int,
        market: str,
        output_dir: str | Path,
    ) -> list[Path]:
        """
        Download all documents for an issuer.

        Args:
            ticker: Ticker symbol
            issuer_id: Issuer ID
            market: Market type
            output_dir: Directory to save files

        Returns:
            List of paths to downloaded files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded = []
        documents = self.get_all_documents(ticker, issuer_id, market)

        for i, doc in enumerate(documents):
            filepath = self.download_document(doc, output_dir)
            if filepath:
                downloaded.append(filepath)

            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i + 1}/{len(documents)} documents")

        return downloaded
