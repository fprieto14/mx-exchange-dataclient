"""
Mexican Exchange Data Client - Python client for BIVA and BMV

This package provides tools to fetch data from Mexican stock exchanges:

BIVA (Bolsa Institucional de Valores):
- REST API client for issuer data, documents, and securities
- CKDs, FIBRAs, CERPIs, and other financial instruments

BMV (Bolsa Mexicana de Valores):
- HTML scraping client for issuer profiles and documents
- Requires beautifulsoup4: pip install mx-exchange-dataclient[scraper]

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
"""

from mx_exchange_dataclient.client import BIVAClient
from mx_exchange_dataclient.models import (
    Document,
    DocumentType,
    Emission,
    Issuer,
    IssuerSummary,
    Security,
)

# BMV imports - optional, requires beautifulsoup4
try:
    from mx_exchange_dataclient.bmv_client import BMVClient
    from mx_exchange_dataclient.bmv_models import (
        BMVDocument,
        BMVIssuer,
        BMVSecurity,
    )

    _BMV_AVAILABLE = True
except ImportError:
    _BMV_AVAILABLE = False
    BMVClient = None  # type: ignore[misc, assignment]
    BMVDocument = None  # type: ignore[misc, assignment]
    BMVIssuer = None  # type: ignore[misc, assignment]
    BMVSecurity = None  # type: ignore[misc, assignment]

__version__ = "0.1.0"
__all__ = [
    # BIVA
    "BIVAClient",
    "Issuer",
    "IssuerSummary",
    "Security",
    "Emission",
    "Document",
    "DocumentType",
    # BMV
    "BMVClient",
    "BMVIssuer",
    "BMVDocument",
    "BMVSecurity",
]
