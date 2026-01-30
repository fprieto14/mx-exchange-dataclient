"""Integration tests for BIVA client.

These tests make real API calls. Run with:
    pytest tests/test_client.py -v --integration

Skip by default in CI.
"""

import pytest

from biva_client import BIVAClient


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    return BIVAClient()


class TestBIVAClientIntegration:
    """Integration tests that hit the real BIVA API."""

    def test_get_issuer(self, client):
        """Test fetching CAPGLPI issuer info."""
        issuer = client.get_issuer(2215)

        assert issuer.id == 2215
        assert issuer.clave == "CAPGLPI"
        assert issuer.estatus == "Activa"
        assert "BANCO" in issuer.razon_social.upper()

    def test_get_issuer_by_name(self, client):
        """Test fetching issuer by name."""
        issuer = client.get_issuer("CAPGLPI")
        assert issuer.id == 2215

    def test_get_securities(self, client):
        """Test fetching securities."""
        securities = client.get_issuer_securities(2215)

        assert len(securities) > 0
        # CAPGLPI has multiple series
        names = [s.nombre for s in securities]
        assert any("CAPGLPI" in name for name in names)

    def test_get_document_types(self, client):
        """Test fetching document types."""
        doc_types = client.get_document_types(2215)

        assert len(doc_types) > 0
        names = [dt.nombre for dt in doc_types]
        # Should have common document types
        assert any("trimestral" in name.lower() for name in names)

    def test_get_documents_single_page(self, client):
        """Test fetching first page of documents."""
        response = client.get_documents(2215, page=0, size=10)

        assert response.total_elements > 0
        assert len(response.content) <= 10
        assert response.total_pages > 0

    def test_get_document_count(self, client):
        """Test getting document count."""
        count = client.get_document_count(2215)

        # CAPGLPI has >1000 documents
        assert count > 1000

    def test_iter_documents_limited(self, client):
        """Test iterating documents with page limit."""
        docs = list(client.iter_documents(2215, max_pages=1))

        assert len(docs) > 0
        assert len(docs) <= 100  # Max page size

        # Check document structure
        doc = docs[0]
        assert doc.id is not None
        assert doc.tipo_documento is not None
        assert doc.download_url.startswith("https://biva.mx")

    def test_get_instrument_types(self, client):
        """Test fetching instrument types."""
        types = client.get_instrument_types()

        assert len(types) > 0
        names = [t["nombre"] for t in types]
        # Should include CKDs and FIBRAs
        assert any("CKD" in name.upper() or "CERPI" in name.upper() for name in names)
        assert any("FIBRA" in name.upper() for name in names)

    def test_get_sectors(self, client):
        """Test fetching sectors."""
        sectors = client.get_sectors()

        assert len(sectors) > 0
        names = [s["nombre"] for s in sectors]
        assert any("financiero" in name.lower() for name in names)
