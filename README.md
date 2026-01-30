# BIVA Client

Python client for **BIVA** (Bolsa Institucional de Valores) - Mexico's second stock exchange.

Fetch issuer data, regulatory filings, and documents for CKDs, FIBRAs, CERPIs, and other Mexican financial instruments.

## Features

- **Direct API access** - No browser automation, pure HTTP requests
- **Typed models** - Pydantic models for all API responses
- **Pagination handling** - Automatic pagination for large document sets
- **Rate limiting** - Built-in delays to respect server limits
- **CLI included** - Command-line interface for quick access
- **Download support** - Bulk download of regulatory filings (PDFs)

## Installation

```bash
pip install biva-client
```

For development:

```bash
pip install biva-client[dev]
```

## Quick Start

### Python API

```python
from biva_client import BIVAClient

client = BIVAClient()

# Get issuer info
issuer = client.get_issuer(2215)  # CAPGLPI
print(f"{issuer.clave}: {issuer.razon_social}")

# Get securities (series)
securities = client.get_issuer_securities(2215)
for sec in securities:
    print(f"  - {sec.nombre}")

# Get document count
total = client.get_document_count(2215)
print(f"Total documents: {total}")

# Iterate over all documents
for doc in client.iter_documents(2215):
    print(f"{doc.fecha_publicacion}: {doc.tipo_documento}")

# Download all documents
paths = client.download_all_documents(2215, "./downloads")
```

### CLI

```bash
# Get issuer info
biva issuer 2215
biva issuer CAPGLPI --securities --emissions

# List documents
biva documents 2215 --types           # Show document type filters
biva documents 2215 --all -o docs.csv # Export all to CSV

# Download documents
biva output 2215 -o ./capglpi/pdfs

# Full export (info + docs + PDFs)
biva export 2215 -o ./capglpi --output
```

## API Reference

### BIVAClient

Main client class for accessing BIVA data.

```python
client = BIVAClient(
    base_url=None,           # Override API base URL
    timeout=30,              # Request timeout (seconds)
    rate_limit_delay=0.5,    # Delay between paginated requests
)
```

#### Issuer Methods

| Method | Description |
|--------|-------------|
| `get_issuer(id)` | Get full issuer details |
| `get_issuer_securities(id)` | Get all securities (valores) |
| `get_issuer_emissions(id)` | Get emissions with pagination |
| `get_all_emissions(id)` | Get all emissions (handles pagination) |

#### Document Methods

| Method | Description |
|--------|-------------|
| `get_document_types(id)` | Get available document type filters |
| `get_documents(id, page, size)` | Get documents (single page) |
| `iter_documents(id)` | Iterate all documents (recommended) |
| `get_all_documents(id)` | Get all documents as list |
| `get_document_count(id)` | Get total document count |

#### Download Methods

| Method | Description |
|--------|-------------|
| `download_document(doc, dir)` | Download single document |
| `download_all_documents(id, dir)` | Download all documents |

#### Reference Data

| Method | Description |
|--------|-------------|
| `get_instrument_types()` | List instrument types (CKD, FIBRA, etc.) |
| `get_sectors()` | List industry sectors |

### Models

All API responses are validated with Pydantic models:

```python
from biva_client import Issuer, Security, Emission, Document, DocumentType
```

#### Issuer

```python
issuer.id              # int - BIVA internal ID
issuer.clave           # str - Ticker/key (e.g., "CAPGLPI")
issuer.razon_social    # str - Legal name
issuer.estatus         # str - "Activa" / "Cancelada"
issuer.sector          # Sector - Industry sector
issuer.fecha_listado   # datetime - Listing date
```

#### Document

```python
doc.id                 # int - Document ID
doc.tipo_documento     # str - Document type description
doc.fecha_publicacion  # datetime - Publication date
doc.file_name          # str - Original filename
doc.download_url       # str - Full output URL
doc.doc_type           # str - File type (PDF, XBRL, etc.)
```

## Known Issuers

The package includes a registry of known issuers for convenience:

| Name | ID | Type |
|------|----|------|
| CAPGLPI | 2215 | CERPI (Apollo/Lock Capital) |

To use by name:

```python
issuer = client.get_issuer("CAPGLPI")  # Resolves to 2215
```

## Document Types

BIVA documents include:

| Type | Description |
|------|-------------|
| Eventos relevantes | Material events |
| Información anual | Annual reports |
| Información trimestral | Quarterly reports |
| Jurídico corporativo | Corporate legal filings |
| Avisos corporativos | Corporate notices |
| Acuerdos de asamblea | Shareholder meeting resolutions |
| Copia de título | Certificate copies |
| Llamada de capital | Capital calls |
| Canje | Exchange notices |

## Rate Limiting

The client includes built-in rate limiting:

- Default 0.5s delay between paginated requests
- Default 0.3s delay between downloads

Adjust with:

```python
client = BIVAClient(rate_limit_delay=1.0)  # More conservative
```

## Error Handling

```python
from requests.exceptions import HTTPError

try:
    issuer = client.get_issuer(99999)
except HTTPError as e:
    if e.response.status_code == 404:
        print("Issuer not found")
```

## Development

```bash
# Clone
git clone https://github.com/your-org/biva-client.git
cd biva-client

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/
ruff format src/

# Type check
mypy src/
```

## License

MIT License

## Disclaimer

This is an unofficial client. BIVA data is subject to their terms of service.
For official data feeds, contact BIVA directly.
