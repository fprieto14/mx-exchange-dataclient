"""Pydantic models for BIVA, BMV, and XBRL data."""

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

# BMV models (always available - no extra dependencies)
from mx_exchange_dataclient.models.bmv import (
    BMVDocument,
    BMVIssuer,
    BMVSecurity,
    DOC_TYPE_CATEGORIES,
    KNOWN_BMV_ISSUERS,
    MARKET_TYPES,
    resolve_bmv_issuer,
)

# XBRL models
from mx_exchange_dataclient.models.xbrl import (
    NAVReconciliation,
    NAVReconciliationReport,
    PerformanceMetrics,
    ReconciliationAnalysis,
    XBRLData,
)

__all__ = [
    # BIVA models
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
    # BMV models
    "BMVDocument",
    "BMVIssuer",
    "BMVSecurity",
    "DOC_TYPE_CATEGORIES",
    "KNOWN_BMV_ISSUERS",
    "MARKET_TYPES",
    "resolve_bmv_issuer",
    # XBRL models
    "NAVReconciliation",
    "NAVReconciliationReport",
    "PerformanceMetrics",
    "ReconciliationAnalysis",
    "XBRLData",
]
