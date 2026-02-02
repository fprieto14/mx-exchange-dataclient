# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python client for Mexican stock exchanges:
- **BIVA** (Bolsa Institucional de Valores) - REST API client
- **BMV** (Bolsa Mexicana de Valores) - HTML scraping client

Fetches issuer data, regulatory filings, and documents for CKDs, FIBRAs, CERPIs, and other Mexican financial instruments.

## Commands

```bash
# Install for development
pip install -e ".[dev]"

# Install with BMV scraping support
pip install -e ".[scraper]"

# Install everything
pip install -e ".[all]"

# Run all unit tests
pytest

# Run a single test file
pytest tests/test_models.py -v

# Run a single test
pytest tests/test_models.py::TestIssuer::test_parse_basic -v

# Run integration tests (makes real API calls)
pytest --integration

# Lint
ruff check src/
ruff format src/

# Type check
mypy src/
```

## Architecture

```
src/mx_exchange_dataclient/
├── clients/
│   ├── biva.py        # BIVAClient - REST API client with rate limiting and pagination
│   ├── bmv.py         # BMVClient - HTML scraping client using Playwright/BeautifulSoup
│   └── base.py        # Shared client utilities
├── models/
│   ├── biva.py        # Pydantic models for BIVA API + KNOWN_ISSUERS registry
│   ├── bmv.py         # Pydantic models for BMV + KNOWN_BMV_ISSUERS registry
│   └── xbrl.py        # Models for XBRL data and analytics
├── cli/
│   ├── main.py        # Unified CLI (mxdata command)
│   ├── biva.py        # BIVA CLI subcommands
│   └── bmv.py         # BMV CLI subcommands
├── sync/
│   ├── engine.py      # SyncEngine - bulk/incremental document sync
│   ├── state.py       # SyncState - track sync progress
│   ├── storage.py     # StorageLayout - organize downloaded files
│   └── download.py    # DownloadManager - handle file downloads
├── xbrl/
│   ├── parser.py      # XBRLParser - parse XBRL financial statements
│   ├── reconciliation.py  # NAV reconciliation across periods
│   └── metrics.py     # Performance metrics (IRR, TVPI, DPI, RVPI)
├── utils/
│   ├── event_classifier.py  # Classify relevant events
│   └── file_organizer.py    # Organize downloaded files by type
├── data/              # Known issuers, XBRL concept mappings
└── __init__.py        # Public API exports
```

### BIVA Client (REST API)

**Key patterns:**
- `BIVAClient` uses `requests.Session` with custom headers (User-Agent, Referer)
- Pagination handled via `iter_documents()` generator (preferred) or `get_all_*()` methods
- Issuer IDs can be numeric (2215) or resolved from `KNOWN_ISSUERS` dict in models.py
- All Pydantic models use `populate_by_name=True` for camelCase API fields
- Timestamps from API are Unix milliseconds, converted via `@field_validator`

**Rate limiting:**
- `rate_limit_delay` (default 0.5s) between paginated requests
- 0.3s delay between document downloads

### BMV Client (HTML Scraping)

**Key patterns:**
- `BMVClient` scrapes HTML pages using Playwright + BeautifulSoup
- Requires `playwright`, `beautifulsoup4`, and `lxml` (install with `pip install mx-exchange-dataclient[scraper]`)
- Issuer identification uses ticker + ID format (e.g., "LOCKXPI-35563")
- Document URLs extracted from anchor tags, converted from viewer URLs to direct downloads
- Market types: CGEN_CAPIT (equities), CGEN_ELDEU (debt), CGEN_GLOB (global), CGEN_CANC (cancelled)

**URL patterns:**
- Issuer profile: `/en/issuers/profile/[TICKER]-[ID]`
- Financial info: `/en/issuers/financialinformation/[TICKER]-[ID]-[MARKET]`
- Relevant events: `/en/issuers/relevantevents/[TICKER]-[ID]-[MARKET]`
- Corporate info: `/en/issuers/corporativeinformation/[TICKER]-[ID]-[MARKET]`

**Document types:**
- `fiduxbrl` - XBRL financial statements (ZIP)
- `constrim` - Quarterly reports (PDF)
- `ratifica` - Auditor ratifications (PDF)
- `anexon` - Annual reports (XBRL ZIP)
- `eventfid` - Relevant events (PDF)

## CLI Usage

### BIVA CLI

```bash
# Get issuer info
biva issuer 2215
biva issuer CAPGLPI --securities --emissions

# List documents
biva documents 2215 --types
biva documents 2215 --all --output documents.csv

# Download all documents
biva output 2215 --output ./downloads/capglpi

# Full export
biva export 2215 --output ./capglpi --output
```

### BMV CLI

```bash
# Get issuer info
bmv issuer LOCKXPI 35563
bmv issuer LOCKXPI 35563 --securities

# List documents
bmv documents LOCKXPI 35563 CGEN_CAPIT
bmv documents LOCKXPI 35563 CGEN_CAPIT --category financial
bmv documents LOCKXPI 35563 CGEN_CAPIT --category events --output docs.csv

# Download all documents
bmv output LOCKXPI 35563 CGEN_CAPIT --output ./downloads

# Full export
bmv export LOCKXPI 35563 CGEN_CAPIT --output ./lockxpi --output
```

## Testing

- `tests/test_models.py` - BIVA model unit tests (no network)
- `tests/test_client.py` - BIVA integration tests marked with `@pytest.mark.integration`
- `tests/test_bmv_models.py` - BMV model unit tests (no network)
- `tests/test_bmv_client.py` - BMV integration tests marked with `@pytest.mark.integration`

Run integration tests with `pytest --integration` (requires network access to biva.mx and bmv.com.mx)

## Known Issuers

### BIVA
| Ticker | ID | Description |
|--------|-----|-------------|
| CAPGLPI | 2215 | Capital Global Private Investment CERPI |

### BMV
| Ticker | ID | Market | Description |
|--------|-----|--------|-------------|
| LOCKXPI | 35563 | CGEN_CAPIT | Lock Capital Private Investment I CKD |
