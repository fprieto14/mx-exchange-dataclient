"""Tests for BIVA models."""

import pytest
from datetime import datetime

from biva_client.models import (
    Issuer,
    Security,
    Document,
    resolve_issuer_id,
    KNOWN_ISSUERS,
)


class TestIssuer:
    """Tests for Issuer model."""

    def test_parse_basic(self):
        data = {
            "id": 2215,
            "clave": "CAPGLPI",
            "razonSocial": "BANCO NACIONAL DE MÉXICO",
            "estatus": "Activa",
            "bolsa": "BIVA",
        }
        issuer = Issuer.model_validate(data)
        assert issuer.id == 2215
        assert issuer.clave == "CAPGLPI"
        assert issuer.razon_social == "BANCO NACIONAL DE MÉXICO"

    def test_parse_timestamp(self):
        data = {
            "id": 1,
            "clave": "TEST",
            "razonSocial": "Test",
            "estatus": "Activa",
            "fechaListado": 1543557600000,  # Unix timestamp in ms
        }
        issuer = Issuer.model_validate(data)
        assert issuer.fecha_listado is not None
        assert isinstance(issuer.fecha_listado, datetime)

    def test_parse_with_sector(self):
        data = {
            "id": 1,
            "clave": "TEST",
            "razonSocial": "Test",
            "estatus": "Activa",
            "sector": {"id": 7, "nombre": "Servicios financieros"},
        }
        issuer = Issuer.model_validate(data)
        assert issuer.sector is not None
        assert issuer.sector.nombre == "Servicios financieros"


class TestSecurity:
    """Tests for Security model."""

    def test_parse(self):
        data = {"id": "MX1RCA1W0001_CAPGLPI-18", "nombre": "1R CAPGLPI 18"}
        security = Security.model_validate(data)
        assert security.isin == "MX1RCA1W0001"
        assert security.ticker == "CAPGLPI-18"


class TestDocument:
    """Tests for Document model."""

    def test_parse(self):
        data = {
            "id": 12345,
            "tipoDocumento": "Avisos corporativos",
            "docType": "PDF",
            "fileName": "aviso.pdf",
            "nombreArchivo": "/storage/docs/aviso.pdf",
            "fechaPublicacion": 1700000000000,
        }
        doc = Document.model_validate(data)
        assert doc.id == 12345
        assert doc.tipo_documento == "Avisos corporativos"
        assert doc.download_url == "https://biva.mx/storage/docs/aviso.pdf"

    def test_download_url_with_leading_slash(self):
        data = {
            "id": 1,
            "tipoDocumento": "Test",
            "docType": "PDF",
            "fileName": "test.pdf",
            "nombreArchivo": "/storage/test.pdf",
        }
        doc = Document.model_validate(data)
        assert doc.download_url == "https://biva.mx/storage/test.pdf"


class TestResolveIssuerId:
    """Tests for issuer ID resolution."""

    def test_resolve_int(self):
        assert resolve_issuer_id(2215) == 2215

    def test_resolve_known_name(self):
        assert resolve_issuer_id("CAPGLPI") == 2215
        assert resolve_issuer_id("capglpi") == 2215  # Case insensitive

    def test_resolve_unknown_name(self):
        with pytest.raises(ValueError, match="Unknown issuer"):
            resolve_issuer_id("UNKNOWN")
