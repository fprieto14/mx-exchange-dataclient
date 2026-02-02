# MX Exchange Data Client

Python client for Mexican stock exchanges: **BIVA** (Bolsa Institucional de Valores) and **BMV** (Bolsa Mexicana de Valores).

Fetch issuer data, regulatory filings, XBRL financial statements, and documents for CKDs, FIBRAs, CERPIs, and other Mexican financial instruments.

## Features

- **BIVA Client** - REST API access for BIVA issuer data and documents
- **BMV Client** - HTML scraping for BMV issuer profiles and filings
- **Sync Engine** - Bulk and incremental document synchronization
- **XBRL Analytics** - Parse financial statements and calculate performance metrics
- **Typed models** - Pydantic models for all API/scraped data
- **CLI included** - Command-line interface for both exchanges
- **Performance metrics** - IRR, TVPI, DPI, RVPI calculations for private equity funds

## Installation

```bash
pip install mx-exchange-dataclient
```

With BMV scraping support (requires Playwright):

```bash
pip install mx-exchange-dataclient[scraper]
```

For development:

```bash
pip install mx-exchange-dataclient[dev]
```

Install everything:

```bash
pip install mx-exchange-dataclient[all]
```

## Quick Start

### BIVA Client

```python
from mx_exchange_dataclient import BIVAClient

client = BIVAClient()

# Get issuer info
issuer = client.get_issuer(2215)  # CAPGLPI
print(f"{issuer.clave}: {issuer.razon_social}")

# Get securities (series)
securities = client.get_issuer_securities(2215)
for sec in securities:
    print(f"  - {sec.nombre}")

# Iterate over all documents
for doc in client.iter_documents(2215):
    print(f"{doc.fecha_publicacion}: {doc.tipo_documento}")

# Download all documents
paths = client.download_all_documents(2215, "./downloads")
```

### BMV Client

```python
from mx_exchange_dataclient import BMVClient

client = BMVClient()

# Get issuer info
issuer = client.get_issuer("LOCKXPI", 35563)
print(f"{issuer.ticker}: {issuer.name}")

# Get financial documents
docs = client.get_financial_documents("LOCKXPI", 35563, "CGEN_CAPIT")
for doc in docs:
    print(f"{doc.date}: {doc.title}")

# Download documents
client.download_documents(docs, "./downloads")
```

### Sync Engine

```python
from mx_exchange_dataclient import SyncEngine

# Initialize sync engine
engine = SyncEngine(output_dir="./data")

# Full sync - download all documents
engine.sync("CAPGLPI", mode="full")

# Incremental sync - only new documents
engine.sync("CAPGLPI", mode="incremental")

# XBRL-only sync - just financial statements
engine.sync("CAPGLPI", mode="xbrl_only")
```

### XBRL Analytics

```python
from mx_exchange_dataclient import XBRLParser, nav_reconciliation, xirr

# Parse XBRL file
parser = XBRLParser()
data = parser.parse("path/to/quarterly_report.xbrl")
print(f"NAV: {data.nav:,.0f}")
print(f"Total Assets: {data.total_assets:,.0f}")

# NAV reconciliation across periods
report = nav_reconciliation("./data/CAPGLPI/xbrl/")
for rec in report.reconciliations:
    print(f"{rec.period}: NAV={rec.nav:,.0f}, Diff={rec.reconciliation_diff:,.0f}")

# Calculate IRR from cash flows
cash_flows = [
    (-1000000, "2020-01-15"),  # Capital call
    (-500000, "2020-06-01"),   # Capital call
    (200000, "2021-12-15"),    # Distribution
    (1800000, "2023-06-30"),   # Current NAV
]
irr = xirr(cash_flows)
print(f"IRR: {irr:.1%}")
```

### CLI

```bash
# BIVA commands
biva issuer 2215
biva issuer CAPGLPI --securities --emissions
biva documents 2215 --types
biva documents 2215 --all -o docs.csv
biva download 2215 -o ./capglpi/pdfs

# BMV commands
bmv issuer LOCKXPI 35563
bmv documents LOCKXPI 35563 CGEN_CAPIT
bmv documents LOCKXPI 35563 CGEN_CAPIT --category financial
bmv download LOCKXPI 35563 CGEN_CAPIT -o ./lockxpi

# Unified CLI
mxdata sync CAPGLPI --mode incremental
mxdata sync LOCKXPI --source bmv --mode xbrl_only
```

## API Reference

### BIVAClient

REST API client for BIVA (Bolsa Institucional de Valores).

```python
from mx_exchange_dataclient import BIVAClient

client = BIVAClient(
    base_url=None,           # Override API base URL
    timeout=30,              # Request timeout (seconds)
    rate_limit_delay=0.5,    # Delay between paginated requests
)
```

| Method | Description |
|--------|-------------|
| `get_issuer(id)` | Get full issuer details |
| `get_issuer_securities(id)` | Get all securities (valores) |
| `iter_documents(id)` | Iterate all documents (recommended) |
| `get_document_count(id)` | Get total document count |
| `download_all_documents(id, dir)` | Download all documents |

### BMVClient

HTML scraping client for BMV (Bolsa Mexicana de Valores). Requires `[scraper]` extras.

```python
from mx_exchange_dataclient import BMVClient

client = BMVClient()
```

| Method | Description |
|--------|-------------|
| `get_issuer(ticker, id)` | Get issuer profile |
| `get_securities(ticker, id)` | Get securities list |
| `get_financial_documents(ticker, id, market)` | Get financial filings |
| `get_event_documents(ticker, id, market)` | Get relevant events |
| `download_documents(docs, dir)` | Download documents |

### SyncEngine

Bulk and incremental document synchronization.

```python
from mx_exchange_dataclient import SyncEngine

engine = SyncEngine(
    output_dir="./data",     # Base output directory
    state_file=None,         # Custom state file path
)
```

| Method | Description |
|--------|-------------|
| `sync(ticker, mode)` | Sync documents (full/incremental/xbrl_only) |
| `get_state(ticker)` | Get current sync state |
| `reset_state(ticker)` | Reset sync state for ticker |

### XBRLParser

Parse XBRL financial statements.

```python
from mx_exchange_dataclient import XBRLParser

parser = XBRLParser()
data = parser.parse("path/to/file.xbrl")
```

Returns `XBRLData` with fields: `nav`, `total_assets`, `total_liabilities`, `issued_capital`, `management_fee`, etc.

### Analytics Functions

```python
from mx_exchange_dataclient import (
    nav_reconciliation,      # Reconcile NAV across periods
    performance_metrics,     # Calculate TVPI, DPI, RVPI
    xirr,                    # Calculate IRR from cash flows
)
```

### Models

All responses are validated with Pydantic models:

```python
from mx_exchange_dataclient import (
    # BIVA
    Issuer, Security, Document, Emission,
    # BMV
    BMVIssuer, BMVDocument, BMVSecurity,
    # XBRL
    XBRLData, NAVReconciliation, PerformanceMetrics,
)
```

## Known Issuers

The package includes registries of known issuers:

### BIVA Issuers

| Ticker | ID | Type |
|--------|----|------|
| CAPGLPI | 2215 | CERPI |

```python
from mx_exchange_dataclient import resolve_issuer_id
issuer_id = resolve_issuer_id("CAPGLPI")  # Returns 2215
```

### BMV Issuers

| Ticker | ID | Market | Type |
|--------|----|--------|------|
| LOCKXPI | 35563 | CGEN_CAPIT | CKD |

```python
from mx_exchange_dataclient import resolve_bmv_issuer
ticker, issuer_id = resolve_bmv_issuer("LOCKXPI")
```

## Document Types

### BIVA Documents

| Type | Description |
|------|-------------|
| Eventos relevantes | Material events |
| Información anual | Annual reports |
| Información trimestral | Quarterly reports |
| Llamada de capital | Capital calls |

### BMV Documents

| Code | Category | Description |
|------|----------|-------------|
| fiduxbrl | financial | XBRL financial statements |
| constrim | financial | Quarterly reports (PDF) |
| anexon | financial | Annual reports (XBRL) |
| eventfid | events | Relevant events |

## Project Structure

```
src/mx_exchange_dataclient/
├── clients/           # BIVA and BMV API clients
├── models/            # Pydantic models for all data types
├── cli/               # Command-line interfaces
├── sync/              # Sync engine for bulk downloads
├── xbrl/              # XBRL parsing and analytics
├── utils/             # Event classification, file organization
└── data/              # Known issuers, XBRL mappings
```

## Development

```bash
# Clone
git clone https://github.com/your-org/mx-exchange-dataclient.git
cd mx-exchange-dataclient

# Install dev dependencies
pip install -e ".[all]"

# Run tests
pytest

# Run integration tests (requires network)
pytest --integration

# Lint
ruff check src/
ruff format src/

# Type check
mypy src/
```

## License

MIT License

## Disclaimer

This is an unofficial client. Data from BIVA and BMV is subject to their respective terms of service. For official data feeds, contact the exchanges directly.
