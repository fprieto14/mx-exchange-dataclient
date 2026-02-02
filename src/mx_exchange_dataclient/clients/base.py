"""Base protocol for exchange clients."""

from pathlib import Path
from typing import Iterator, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class ExchangeClient(Protocol):
    """Protocol defining the interface for exchange clients.

    Both BIVAClient and BMVClient implement this interface, allowing
    for unified handling of different data sources.
    """

    def get_issuer(self, issuer_id: int | str) -> BaseModel:
        """Get issuer information.

        Args:
            issuer_id: Issuer identifier (format varies by exchange)

        Returns:
            Issuer model (Issuer or BMVIssuer)
        """
        ...

    def get_issuer_securities(self, issuer_id: int | str) -> list[BaseModel]:
        """Get securities for an issuer.

        Args:
            issuer_id: Issuer identifier

        Returns:
            List of Security models
        """
        ...

    def iter_documents(self, issuer_id: int | str, **kwargs) -> Iterator[BaseModel]:
        """Iterate over documents for an issuer.

        Args:
            issuer_id: Issuer identifier
            **kwargs: Additional filters

        Yields:
            Document models
        """
        ...

    def get_all_documents(self, issuer_id: int | str, **kwargs) -> list[BaseModel]:
        """Get all documents for an issuer.

        Args:
            issuer_id: Issuer identifier
            **kwargs: Additional filters

        Returns:
            List of Document models
        """
        ...

    def download_document(
        self,
        document: BaseModel,
        output_dir: str | Path,
        delay: float = 0.3,
    ) -> Path | None:
        """Download a document file.

        Args:
            document: Document model to download
            output_dir: Directory to save file
            delay: Delay before download (rate limiting)

        Returns:
            Path to downloaded file, or None if failed
        """
        ...

    def download_all_documents(
        self,
        issuer_id: int | str,
        output_dir: str | Path,
        **kwargs,
    ) -> list[Path]:
        """Download all documents for an issuer.

        Args:
            issuer_id: Issuer identifier
            output_dir: Directory to save files
            **kwargs: Additional options

        Returns:
            List of paths to downloaded files
        """
        ...
