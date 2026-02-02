"""Tests for BMV models."""

import pytest

from mx_exchange_dataclient.models.bmv import (
    BMVDocument,
    BMVIssuer,
    BMVSecurity,
    KNOWN_BMV_ISSUERS,
    resolve_bmv_issuer,
)


class TestBMVIssuer:
    """Tests for BMVIssuer model."""

    def test_parse_basic(self):
        data = {
            "ticker": "LOCKXPI",
            "id": 35563,
            "name": "Lock Capital Private Investment I",
            "market": "CGEN_CAPIT",
            "status": "ACTIVA",
        }
        issuer = BMVIssuer.model_validate(data)
        assert issuer.ticker == "LOCKXPI"
        assert issuer.id == 35563
        assert issuer.market == "CGEN_CAPIT"
        assert issuer.status == "ACTIVA"

    def test_profile_url(self):
        issuer = BMVIssuer(
            ticker="LOCKXPI",
            id=35563,
            name="Test",
            market="CGEN_CAPIT",
            status="ACTIVA",
        )
        assert issuer.profile_url == "https://www.bmv.com.mx/en/issuers/profile/LOCKXPI-35563"


class TestBMVSecurity:
    """Tests for BMVSecurity model."""

    def test_parse(self):
        data = {"name": "LOCKXPI 24-3", "series": "24-3", "status": "ACTIVA"}
        security = BMVSecurity.model_validate(data)
        assert security.name == "LOCKXPI 24-3"
        assert security.series == "24-3"
        assert security.status == "ACTIVA"


class TestBMVDocument:
    """Tests for BMVDocument model."""

    def test_parse_pdf(self):
        data = {
            "id": "constrim_1528122_6199_2025-03_1_pdf",
            "doc_type": "constrim",
            "category": "Quarterly Reports",
            "filename": "constrim_1528122_6199_2025-03_1.pdf",
            "url": "https://www.bmv.com.mx/docs-pub/constrim/constrim_1528122_6199_2025-03_1.pdf",
            "period": "2025-03",
            "is_xbrl": False,
        }
        doc = BMVDocument.model_validate(data)
        assert doc.id == "constrim_1528122_6199_2025-03_1_pdf"
        assert doc.doc_type == "constrim"
        assert doc.period == "2025-03"
        assert doc.is_pdf is True
        assert doc.is_xbrl is False
        assert doc.download_url == data["url"]

    def test_parse_xbrl_viewer(self):
        data = {
            "id": "fiduxbrl_1528121_6199_2025-03_1_zip",
            "doc_type": "fiduxbrl",
            "category": "Financial Statements (XBRL)",
            "filename": "fiduxbrl_1528121_6199_2025-03_1.zip",
            "url": "https://www.bmv.com.mx/docs-pub/visor/visorXbrl.html?docins=../fiduxbrl/fiduxbrl_1528121_6199_2025-03_1.zip",
            "period": "2025-03",
            "is_xbrl": True,
        }
        doc = BMVDocument.model_validate(data)
        assert doc.is_xbrl is True
        assert doc.is_pdf is False
        # Viewer URL should be converted to direct ZIP URL
        assert doc.download_url == "https://www.bmv.com.mx/docs-pub/fiduxbrl/fiduxbrl_1528121_6199_2025-03_1.zip"

    def test_download_url_direct_pdf(self):
        doc = BMVDocument(
            id="test_pdf",
            doc_type="eventfid",
            category="Relevant Events",
            filename="eventfid_123.pdf",
            url="https://www.bmv.com.mx/docs-pub/eventfid/eventfid_123.pdf",
        )
        assert doc.download_url == "https://www.bmv.com.mx/docs-pub/eventfid/eventfid_123.pdf"


class TestResolveBMVIssuer:
    """Tests for BMV issuer resolution."""

    def test_resolve_known_ticker(self):
        ticker, issuer_id = resolve_bmv_issuer("LOCKXPI")
        assert ticker == "LOCKXPI"
        assert issuer_id == 35563

    def test_resolve_case_insensitive(self):
        ticker, issuer_id = resolve_bmv_issuer("lockxpi")
        assert ticker == "LOCKXPI"
        assert issuer_id == 35563

    def test_resolve_with_id(self):
        ticker, issuer_id = resolve_bmv_issuer("NEWTICKER-12345")
        assert ticker == "NEWTICKER"
        assert issuer_id == 12345

    def test_resolve_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown BMV issuer"):
            resolve_bmv_issuer("UNKNOWN")
