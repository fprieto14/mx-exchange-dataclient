"""Pydantic models for BMV (Bolsa Mexicana de Valores) scraped data."""

from pydantic import BaseModel, computed_field


class BMVIssuer(BaseModel):
    """BMV issuer information scraped from profile page."""

    ticker: str  # e.g., "LOCKXPI"
    id: int  # e.g., 35563
    name: str  # Razón social
    market: str  # CGEN_CAPIT, CGEN_ELDEU, etc.
    status: str  # "ACTIVA", "CANCELADA"

    model_config = {"populate_by_name": True}

    @property
    def profile_url(self) -> str:
        """Get the issuer profile URL."""
        return f"https://www.bmv.com.mx/en/issuers/profile/{self.ticker}-{self.id}"


class BMVSecurity(BaseModel):
    """Security listed under a BMV issuer."""

    name: str
    series: str
    status: str

    model_config = {"populate_by_name": True}


class BMVDocument(BaseModel):
    """Document from BMV docs-pub."""

    id: str  # Extracted from filename
    doc_type: str  # "fiduxbrl", "constrim", "eventfid", etc.
    category: str  # "Financial Statements", "Relevant Events", etc.
    filename: str
    url: str  # Full URL (may be viewer URL or direct URL)
    period: str | None = None  # "2025-03", "2024", etc.
    is_xbrl: bool = False  # True for XBRL ZIP files

    model_config = {"populate_by_name": True}

    @computed_field
    @property
    def download_url(self) -> str:
        """Get direct output URL (converts viewer URL to ZIP URL if needed)."""
        if "visorXbrl.html" in self.url:
            # Extract ZIP path from viewer URL
            # e.g., visor/visorXbrl.html?docins=../fiduxbrl/fiduxbrl_123.zip
            # -> /fiduxbrl/fiduxbrl_123.zip
            return self.url.replace("/visor/visorXbrl.html?docins=../", "/")
        return self.url

    @property
    def is_pdf(self) -> bool:
        """Check if document is a PDF."""
        return self.filename.lower().endswith(".pdf")


# Document type to category mapping
DOC_TYPE_CATEGORIES = {
    "fiduxbrl": "Financial Statements (XBRL)",
    "constrim": "Quarterly Reports",
    "ratifica": "Auditor Ratifications",
    "anexon": "Annual Reports (XBRL)",
    "eventfid": "Relevant Events",
    "asamble": "Assemblies",
    "convoca": "Notices",
    "dictam": "Opinions",
    "prospec": "Prospectus",
    "suplem": "Supplements",
}


# Market type descriptions
MARKET_TYPES = {
    "CGEN_CAPIT": "Capitales (Equities, CKDs)",
    "CGEN_ELDEU": "Deuda (Debt)",
    "CGEN_GLOB": "Global",
    "CGEN_CANC": "Cancelled",
}


# Known BMV issuers for convenience (ticker -> id mapping)
KNOWN_BMV_ISSUERS: dict[str, int] = {
    "LOCKXPI": 35563,  # Lock Capital Private Investment I CKD
    # Add more as discovered
}


def resolve_bmv_issuer(ticker: str) -> tuple[str, int]:
    """
    Resolve BMV issuer ticker to (ticker, id) tuple.

    Args:
        ticker: Ticker symbol (e.g., "LOCKXPI") or "TICKER-ID" format

    Returns:
        Tuple of (ticker, id)

    Raises:
        ValueError: If ticker not found and no ID provided
    """
    upper = ticker.upper()

    # Check if ID is included in the string (e.g., "LOCKXPI-35563")
    if "-" in upper and upper.split("-")[-1].isdigit():
        parts = upper.rsplit("-", 1)
        return parts[0], int(parts[1])

    # Look up in known issuers
    if upper in KNOWN_BMV_ISSUERS:
        return upper, KNOWN_BMV_ISSUERS[upper]

    raise ValueError(
        f"Unknown BMV issuer: {ticker}. Use format 'TICKER-ID' or add to KNOWN_BMV_ISSUERS."
    )
