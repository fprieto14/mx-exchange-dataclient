"""
BIVA API Client

Direct HTTP client for BIVA's REST API endpoints.
No browser automation required - pure requests.
"""

import logging
import time
from pathlib import Path
from typing import Callable, Iterator

import requests

from mx_exchange_dataclient.models import (
    Document,
    DocumentType,
    Emission,
    Issuer,
    PaginatedResponse,
    Security,
    resolve_issuer_id,
)

logger = logging.getLogger(__name__)


class BIVAClient:
    """
    Client for BIVA (Bolsa Institucional de Valores) REST API.

    Example:
        >>> client = BIVAClient()
        >>> issuer = client.get_issuer(2215)
        >>> print(issuer.clave)
        CAPGLPI

        >>> for doc in client.iter_documents(2215):
        ...     print(doc.tipo_documento, doc.download_url)
    """

    BASE_URL = "https://www.biva.mx/emisoras"
    STORAGE_BASE = "https://biva.mx"

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = 30,
        rate_limit_delay: float = 0.5,
    ):
        """
        Initialize BIVA client.

        Args:
            base_url: Override base API URL
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between paginated requests (seconds)
        """
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "biva-client/0.1.0",
            "Accept": "application/json",
            "Referer": "https://www.biva.mx/empresas/emisoras_inscritas",
        })

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make GET request to API endpoint."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # Issuer Methods
    # -------------------------------------------------------------------------

    def get_issuer(self, issuer_id: int | str) -> Issuer:
        """
        Get detailed issuer information.

        Args:
            issuer_id: Numeric ID or known name (e.g., "CAPGLPI")

        Returns:
            Issuer object with full details
        """
        issuer_id = resolve_issuer_id(issuer_id)
        data = self._get(f"empresas/{issuer_id}")
        return Issuer.model_validate(data)

    def get_issuer_securities(self, issuer_id: int | str) -> list[Security]:
        """
        Get all securities (valores) for an issuer.

        Args:
            issuer_id: Numeric ID or known name

        Returns:
            List of Security objects
        """
        issuer_id = resolve_issuer_id(issuer_id)
        data = self._get(f"empresas/{issuer_id}/valores", {"cotizacion": "true"})
        return [Security.model_validate(item) for item in data]

    def get_issuer_emissions(
        self,
        issuer_id: int | str,
        page: int = 0,
        size: int = 100,
    ) -> PaginatedResponse:
        """
        Get emission details for an issuer.

        Args:
            issuer_id: Numeric ID or known name
            page: Page number (0-indexed)
            size: Page size

        Returns:
            Paginated response with Emission objects
        """
        issuer_id = resolve_issuer_id(issuer_id)
        data = self._get(
            f"empresas/{issuer_id}/emisiones",
            {"page": page, "size": size, "cotizacion": "true"},
        )
        response = PaginatedResponse.model_validate(data)
        response.content = [Emission.model_validate(item) for item in response.content]
        return response

    def get_all_emissions(self, issuer_id: int | str) -> list[Emission]:
        """Get all emissions for an issuer (handles pagination)."""
        issuer_id = resolve_issuer_id(issuer_id)
        all_emissions = []
        page = 0

        while True:
            response = self.get_issuer_emissions(issuer_id, page=page, size=100)
            all_emissions.extend(response.content)

            if page + 1 >= response.total_pages:
                break

            page += 1
            time.sleep(self.rate_limit_delay)

        return all_emissions

    # -------------------------------------------------------------------------
    # Document Methods
    # -------------------------------------------------------------------------

    def get_document_types(self, issuer_id: int | str) -> list[DocumentType]:
        """
        Get available document types for an issuer.

        Args:
            issuer_id: Numeric ID or known name

        Returns:
            List of DocumentType objects
        """
        issuer_id = resolve_issuer_id(issuer_id)
        data = self._get(f"empresas/{issuer_id}/tipo-informacion")
        return [DocumentType.model_validate(item) for item in data]

    def get_documents(
        self,
        issuer_id: int | str,
        page: int = 0,
        size: int = 100,
        tipo_informacion: str | None = None,
        tipo_documento: str | None = None,
    ) -> PaginatedResponse:
        """
        Get documents for an issuer (single page).

        Args:
            issuer_id: Numeric ID or known name
            page: Page number (0-indexed)
            size: Page size (max 100)
            tipo_informacion: Filter by information type ID
            tipo_documento: Filter by document type ID

        Returns:
            Paginated response with Document objects
        """
        issuer_id = resolve_issuer_id(issuer_id)
        params = {"page": page, "size": size}

        if tipo_informacion:
            params["tipoInformacion"] = tipo_informacion
        if tipo_documento:
            params["tipoDocumento"] = tipo_documento

        data = self._get(f"empresas/{issuer_id}/documentos", params)
        response = PaginatedResponse.model_validate(data)
        response.content = [Document.model_validate(item) for item in response.content]
        return response

    def iter_documents(
        self,
        issuer_id: int | str,
        max_pages: int | None = None,
        **kwargs,
    ) -> Iterator[Document]:
        """
        Iterate over all documents for an issuer.

        This is the recommended way to fetch all documents as it handles
        pagination automatically and yields documents one at a time.

        Args:
            issuer_id: Numeric ID or known name
            max_pages: Limit number of pages (for testing)
            **kwargs: Additional filters (tipo_informacion, tipo_documento)

        Yields:
            Document objects
        """
        issuer_id = resolve_issuer_id(issuer_id)
        page = 0

        while True:
            response = self.get_documents(issuer_id, page=page, size=100, **kwargs)

            for doc in response.content:
                yield doc

            if max_pages and page + 1 >= max_pages:
                break

            if page + 1 >= response.total_pages:
                break

            page += 1
            time.sleep(self.rate_limit_delay)

    def get_all_documents(
        self,
        issuer_id: int | str,
        max_pages: int | None = None,
        **kwargs,
    ) -> list[Document]:
        """
        Get all documents for an issuer.

        Args:
            issuer_id: Numeric ID or known name
            max_pages: Limit number of pages
            **kwargs: Additional filters

        Returns:
            List of all Document objects
        """
        return list(self.iter_documents(issuer_id, max_pages=max_pages, **kwargs))

    def get_document_count(self, issuer_id: int | str) -> int:
        """Get total document count for an issuer."""
        issuer_id = resolve_issuer_id(issuer_id)
        response = self.get_documents(issuer_id, page=0, size=1)
        return response.total_elements

    # -------------------------------------------------------------------------
    # Download Methods
    # -------------------------------------------------------------------------

    def download_document(
        self,
        document: Document,
        output_dir: str | Path,
        delay: float = 0.3,
    ) -> Path | None:
        """
        Download a document file.

        Args:
            document: Document object to output
            output_dir: Directory to save file
            delay: Delay before output (rate limiting)

        Returns:
            Path to downloaded file, or None if failed
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        url = document.download_url

        # Generate filename with date prefix
        fecha_str = ""
        if document.fecha_publicacion:
            fecha_str = document.fecha_publicacion.strftime("%Y%m%d_")

        safe_tipo = "".join(
            c for c in document.tipo_documento if c.isalnum() or c in " _-"
        )[:50]
        filename = f"{fecha_str}{safe_tipo}_{document.file_name}"
        filepath = output_dir / filename

        # Skip if exists
        if filepath.exists():
            logger.debug(f"Already exists: {filename}")
            return filepath

        try:
            time.sleep(delay)
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded: {filename}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to output {url}: {e}")
            return None

    def download_all_documents(
        self,
        issuer_id: int | str,
        output_dir: str | Path,
        max_pages: int | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> list[Path]:
        """
        Download all documents for an issuer.

        Args:
            issuer_id: Numeric ID or known name
            output_dir: Directory to save files
            max_pages: Limit document pages
            progress_callback: Called with (current, total) for progress updates

        Returns:
            List of paths to downloaded files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded = []
        total = self.get_document_count(issuer_id)

        for i, doc in enumerate(self.iter_documents(issuer_id, max_pages=max_pages)):
            filepath = self.download_document(doc, output_dir)
            if filepath:
                downloaded.append(filepath)

            if progress_callback:
                progress_callback(i + 1, total)

            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{total} documents")

        return downloaded

    # -------------------------------------------------------------------------
    # Reference Data Methods
    # -------------------------------------------------------------------------

    def get_instrument_types(self) -> list[dict]:
        """Get all instrument types available on BIVA."""
        return self._get("tipo-instrumento", {"biva": "true"})

    def get_sectors(self) -> list[dict]:
        """Get all sectors."""
        return self._get("sectores", {"biva": "true"})

    def get_inscription_types(self) -> list[dict]:
        """Get inscription types (tradicional, simplificada)."""
        return self._get("tipo-inscripcion")
