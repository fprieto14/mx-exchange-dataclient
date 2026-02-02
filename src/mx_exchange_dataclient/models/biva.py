"""Pydantic models for BIVA API responses."""

from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, Field, field_validator


class Sector(BaseModel):
    """Industry sector classification."""

    id: int
    nombre: str

    @property
    def name(self) -> str:
        return self.nombre


class Issuer(BaseModel):
    """Full issuer (emisora) information."""

    id: int
    clave: str
    razon_social: str = Field(alias="razonSocial")
    bolsa: str = "BIVA"
    estatus: str
    direccion: str | None = None
    telefono: str | None = None
    sitio_web: str | None = Field(default=None, alias="sitioWeb")
    logo: str | None = None
    fecha_listado: datetime | None = Field(default=None, alias="fechaListado")
    sector: Sector | None = None
    subsector: Sector | None = None
    ramo: Sector | None = None
    subramo: Sector | None = None
    es_simplificada: bool = Field(default=False, alias="esSimplificada")

    @field_validator("fecha_listado", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, int):
            return datetime.fromtimestamp(v / 1000)
        return v

    model_config = {"populate_by_name": True}


class IssuerSummary(BaseModel):
    """Summary issuer info from listings."""

    id: int
    clave: str
    nombre: str | None = None
    estatus: str | None = None


class Security(BaseModel):
    """Security (valor) information."""

    id: str
    nombre: str

    @property
    def isin(self) -> str:
        """Extract ISIN from ID."""
        return self.id.split("_")[0] if "_" in self.id else self.id

    @property
    def ticker(self) -> str:
        """Extract ticker from ID."""
        return self.id.split("_")[1] if "_" in self.id else self.nombre


class Emission(BaseModel):
    """Emission (emisión) details."""

    id: int
    serie: str
    isin: str
    nombre: str
    tipo_valor: str = Field(alias="tipoValor")
    clave_tipo_valor: str = Field(alias="claveTipoValor")
    tipo_instrumento: str = Field(alias="tipoInstrumento")
    id_tipo_instrumento: int = Field(alias="idTipoInstrumento")
    tipo_emision: str = Field(alias="tipoEmision")
    modo_listado: str = Field(alias="modoListado")
    id_modo_listado: int = Field(alias="idModoListado")
    representante_comun: str | None = Field(default=None, alias="representanteComun")
    fecha_emision: datetime | None = Field(default=None, alias="fechaEmision")
    fecha_vencimiento: datetime | None = Field(default=None, alias="fechaVencimiento")
    titulos_en_circulacion: int | None = Field(default=None, alias="titulosEnCirculacion")

    @field_validator("fecha_emision", "fecha_vencimiento", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, int):
            return datetime.fromtimestamp(v / 1000)
        return v

    model_config = {"populate_by_name": True}


class DocumentFile(BaseModel):
    """Individual file within a document."""

    url: str
    file_type: str | None = Field(default=None, alias="fileType")
    file_name: str | None = Field(default=None, alias="fileName")
    extension: str | None = None  # New API format uses extension instead

    model_config = {"populate_by_name": True}

    @property
    def ext(self) -> str:
        """Get file extension from either format."""
        if self.extension:
            return self.extension.lower()
        if self.file_type:
            return self.file_type.lower()
        return ""


class Document(BaseModel):
    """Regulatory document/filing."""

    id: int
    tipo_documento: str = Field(alias="tipoDocumento")
    doc_type: str = Field(alias="docType")
    file_name: str = Field(alias="fileName")
    nombre_archivo: str = Field(alias="nombreArchivo")
    fecha_publicacion: datetime | None = Field(default=None, alias="fechaPublicacion")
    fecha_creacion: datetime | None = Field(default=None, alias="fechaCreacion")
    archivos: list[DocumentFile] = Field(default_factory=list)
    archivos_xbrl: list[DocumentFile] = Field(default_factory=list, alias="archivosXbrl")

    @field_validator("fecha_publicacion", "fecha_creacion", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, int):
            return datetime.fromtimestamp(v / 1000)
        return v

    @property
    def download_url(self) -> str:
        """Get the full download URL for this document."""
        base = "https://biva.mx"
        path = self.nombre_archivo

        # Handle XBRL viewer URLs - extract actual file path from query params
        if "visorxbrl/index.html" in path:
            parsed = urlparse(path)
            params = parse_qs(parsed.query)
            # Get XBRL file path from documentPathXbrl parameter
            if "documentPathXbrl" in params:
                xbrl_path = params["documentPathXbrl"][0]
                return f"{base}{xbrl_path}"

        if path.startswith("/"):
            return f"{base}{path}"
        return f"{base}/{path}"

    @property
    def xbrl_url(self) -> str | None:
        """Get the actual XBRL file URL from archivos_xbrl if available."""
        base = "https://biva.mx"
        for f in self.archivos_xbrl:
            if f.ext == "xbrl":
                url = f.url
                if url.startswith("/"):
                    return f"{base}{url}"
                return f"{base}/{url}"
        return None

    def get_all_download_urls(self) -> dict[str, str]:
        """Get all available download URLs for this document.

        Returns dict with keys like 'pdf', 'xbrl', 'xlsx', 'docx', 'html'.
        """
        base = "https://biva.mx"
        urls = {}

        # Main document
        if self.doc_type:
            urls[self.doc_type.lower()] = self.download_url

        # Additional files from archivos_xbrl
        for f in self.archivos_xbrl:
            ext = f.ext
            if ext:
                url = f.url
                if url.startswith("/"):
                    urls[ext] = f"{base}{url}"
                else:
                    urls[ext] = f"{base}/{url}"

        return urls

    model_config = {"populate_by_name": True}


class DocumentType(BaseModel):
    """Document type category."""

    id: str
    nombre: str
    tipo: str | None = None

    @property
    def name(self) -> str:
        return self.nombre


class PaginatedResponse(BaseModel):
    """Generic paginated API response."""

    content: list[Any]
    number: int
    size: int
    total_elements: int = Field(alias="totalElements")
    total_pages: int = Field(alias="totalPages")

    model_config = {"populate_by_name": True}


# Known issuer IDs for convenience
KNOWN_ISSUERS: dict[str, int] = {
    "CAPGLPI": 2215,
    "QTZALPI": 2282,
    # Add more as discovered
}


def resolve_issuer_id(issuer: int | str) -> int:
    """Resolve issuer name to ID."""
    if isinstance(issuer, int):
        return issuer
    upper = issuer.upper()
    if upper in KNOWN_ISSUERS:
        return KNOWN_ISSUERS[upper]
    raise ValueError(f"Unknown issuer: {issuer}. Use numeric ID or add to KNOWN_ISSUERS.")
