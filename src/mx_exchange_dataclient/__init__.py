"""
Mexican Exchange Data Client - Python client for BIVA and BMV

This package provides tools to fetch data from Mexican stock exchanges:

BIVA (Bolsa Institucional de Valores):
- REST API client for issuer data, documents, and securities
- CKDs, FIBRAs, CERPIs, and other financial instruments

BMV (Bolsa Mexicana de Valores):
- HTML scraping client for issuer profiles and documents
- Requires beautifulsoup4: pip install mx-exchange-dataclient[scraper]

Sync Engine:
- Bulk and incremental document synchronization
- XBRL-only sync for financial statements
- State tracking for efficient updates

XBRL Analytics:
- Parse XBRL files and extract financial data
- NAV reconciliation and performance metrics
- IRR, TVPI, DPI, RVPI calculations

Example usage (BIVA):
    >>> from mx_exchange_dataclient import BIVAClient
    >>> client = BIVAClient()
    >>> issuer = client.get_issuer(2215)  # CAPGLPI
    >>> print(issuer.clave, issuer.razon_social)

Example usage (BMV):
    >>> from mx_exchange_dataclient import BMVClient
    >>> client = BMVClient()
    >>> issuer = client.get_issuer("LOCKXPI", 35563)
    >>> docs = client.get_financial_documents("LOCKXPI", 35563, "CGEN_CAPIT")

Example usage (SyncEngine):
    >>> from mx_exchange_dataclient import SyncEngine
    >>> engine = SyncEngine(output_dir="./data")
    >>> engine.sync("CAPGLPI", mode="incremental")

Example usage (XBRL Analytics):
    >>> from mx_exchange_dataclient import XBRLParser, nav_reconciliation
    >>> parser = XBRLParser()
    >>> data = parser.parse("path/to/file.xbrl")
"""

__version__ = "0.1.0"

# Clients
from mx_exchange_dataclient.clients.biva import BIVAClient

# BIVA Models
from mx_exchange_dataclient.models.biva import (
    Document,
    DocumentFile,
    DocumentType,
    Emission,
    Issuer,
    IssuerSummary,
    KNOWN_ISSUERS,
    PaginatedResponse,
    Sector,
    Security,
    resolve_issuer_id,
)

# BMV Models (always available - no extra dependencies)
from mx_exchange_dataclient.models.bmv import (
    BMVDocument,
    BMVIssuer,
    BMVSecurity,
    DOC_TYPE_CATEGORIES,
    KNOWN_BMV_ISSUERS,
    MARKET_TYPES,
    resolve_bmv_issuer,
)

# XBRL Models
from mx_exchange_dataclient.models.xbrl import (
    NAVReconciliation,
    NAVReconciliationReport,
    PerformanceMetrics,
    ReconciliationAnalysis,
    XBRLData,
)

# Sync Engine
from mx_exchange_dataclient.sync.engine import SyncEngine
from mx_exchange_dataclient.sync.state import SyncState
from mx_exchange_dataclient.sync.storage import StorageLayout
from mx_exchange_dataclient.sync.download import DownloadManager

# XBRL Parser and Analytics
from mx_exchange_dataclient.xbrl.parser import XBRLParser
from mx_exchange_dataclient.xbrl.reconciliation import (
    nav_reconciliation,
    nav_reconciliation_by_period,
    find_xbrl_files,
    ReportPeriod,
)
from mx_exchange_dataclient.xbrl.metrics import (
    NAVAnalyticsDB,
    PeriodType,
    performance_metrics,
    reconciliation_analysis,
    xirr,
)

# Data utilities
from mx_exchange_dataclient.data import (
    get_issuer_info,
    get_issuer_mapping,
    load_known_issuers,
    load_xbrl_mappings,
)

# BMV client - optional, requires beautifulsoup4
try:
    from mx_exchange_dataclient.clients.bmv import BMVClient

    _BMV_AVAILABLE = True
except ImportError:
    _BMV_AVAILABLE = False
    BMVClient = None  # type: ignore[misc, assignment]


__all__ = [
    # Version
    "__version__",
    # BIVA Client & Models
    "BIVAClient",
    "Document",
    "DocumentFile",
    "DocumentType",
    "Emission",
    "Issuer",
    "IssuerSummary",
    "KNOWN_ISSUERS",
    "PaginatedResponse",
    "Sector",
    "Security",
    "resolve_issuer_id",
    # BMV Client & Models
    "BMVClient",
    "BMVDocument",
    "BMVIssuer",
    "BMVSecurity",
    "DOC_TYPE_CATEGORIES",
    "KNOWN_BMV_ISSUERS",
    "MARKET_TYPES",
    "resolve_bmv_issuer",
    # XBRL Models
    "NAVReconciliation",
    "NAVReconciliationReport",
    "PerformanceMetrics",
    "ReconciliationAnalysis",
    "XBRLData",
    # Sync Engine
    "SyncEngine",
    "SyncState",
    "StorageLayout",
    "DownloadManager",
    # XBRL Parser & Analytics
    "XBRLParser",
    "nav_reconciliation",
    "nav_reconciliation_by_period",
    "find_xbrl_files",
    "ReportPeriod",
    "NAVAnalyticsDB",
    "PeriodType",
    "performance_metrics",
    "reconciliation_analysis",
    "xirr",
    # Data utilities
    "get_issuer_info",
    "get_issuer_mapping",
    "load_known_issuers",
    "load_xbrl_mappings",
]
