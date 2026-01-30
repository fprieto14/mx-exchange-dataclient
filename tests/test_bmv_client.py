"""Integration tests for BMV client.

These tests make real HTTP calls. Run with:
    pytest tests/test_bmv_client.py -v --integration

Skip by default in CI.

Note: Requires beautifulsoup4 and lxml:
    pip install biva-client[scraper]
"""

import pytest

try:
    from biva_client.bmv_client import BMVClient

    HAS_BMV = True
except ImportError:
    HAS_BMV = False


# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not HAS_BMV, reason="BMV dependencies not installed"),
]


@pytest.fixture
def client():
    return BMVClient()


class TestBMVClientIntegration:
    """Integration tests that scrape the real BMV website."""

    def test_get_issuer(self, client):
        """Test fetching LOCKXPI issuer info."""
        issuer = client.get_issuer("LOCKXPI", 35563)

        assert issuer.ticker == "LOCKXPI"
        assert issuer.id == 35563
        assert issuer.name  # Should have some name

    def test_get_issuer_with_known_ticker(self, client):
        """Test fetching issuer by known ticker only."""
        issuer = client.get_issuer("LOCKXPI")
        assert issuer.id == 35563

    def test_get_financial_documents(self, client):
        """Test fetching financial documents."""
        docs = client.get_financial_documents("LOCKXPI", 35563, "CGEN_CAPIT")

        # May or may not have documents, but should not error
        assert isinstance(docs, list)

        if docs:
            doc = docs[0]
            assert doc.filename
            assert doc.download_url.startswith("https://")

    def test_get_relevant_events(self, client):
        """Test fetching relevant events."""
        docs = client.get_relevant_events("LOCKXPI", 35563, "CGEN_CAPIT")

        assert isinstance(docs, list)

    def test_get_all_documents(self, client):
        """Test fetching all documents."""
        docs = client.get_all_documents("LOCKXPI", 35563, "CGEN_CAPIT")

        assert isinstance(docs, list)

    def test_iter_all_documents(self, client):
        """Test iterating all documents."""
        docs = list(client.iter_all_documents("LOCKXPI", 35563, "CGEN_CAPIT"))

        assert isinstance(docs, list)
