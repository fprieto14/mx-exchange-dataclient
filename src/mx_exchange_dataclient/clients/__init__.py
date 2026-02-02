"""Exchange client implementations for BIVA and BMV."""

from mx_exchange_dataclient.clients.biva import BIVAClient

# BMV client requires beautifulsoup4
try:
    from mx_exchange_dataclient.clients.bmv import BMVClient

    _BMV_AVAILABLE = True
except ImportError:
    _BMV_AVAILABLE = False
    BMVClient = None  # type: ignore[misc, assignment]

__all__ = ["BIVAClient", "BMVClient"]
