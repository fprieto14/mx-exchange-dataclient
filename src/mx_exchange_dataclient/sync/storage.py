"""Storage layout management for downloaded documents."""

import re
from pathlib import Path

# Valid ticker pattern: alphanumeric, underscore, hyphen only
VALID_TICKER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class StorageLayout:
    """Manages folder structure for downloaded documents.

    Default structure:
        <output_dir>/
        ├── <ticker>/
        │   ├── ReporteTrimestral/
        │   │   ├── 20250331_ReporteTrimestral_1T_2025_....xbrl
        │   │   └── ...
        │   ├── ReporteAnual/
        │   ├── EventoRelevante/
        │   ├── InformacionFinanciera/
        │   └── Otros/
        ├── .sync_state.json
        └── ...
    """

    # Quarter end dates for standardized naming
    QUARTER_DATES = {
        "1": "0331",
        "1T": "0331",
        "2": "0630",
        "2T": "0630",
        "3": "0930",
        "3T": "0930",
        "4": "1231",
        "4T": "1231",
        "4D": "1231",
        "4DT": "1231",
    }

    # Document type patterns and their folder names
    DOC_TYPE_PATTERNS = [
        (r"ReporteTrimestral", "ReporteTrimestral"),
        (r"ReporteAnual", "ReporteAnual"),
        (r"bivaFR2", "ReporteAnual"),  # BIVA annual reports
        (r"EventoRelevante", "EventoRelevante"),
        (r"InformacionFinanciera", "InformacionFinanciera"),
        (r"InformacionCorporativa", "InformacionCorporativa"),
        (r"Prospecto", "Prospecto"),
        (r"Aviso", "Aviso"),
        (r"Dictamen", "Dictamen"),
    ]

    DEFAULT_DOC_TYPE = "Otros"

    def __init__(self, output_dir: str | Path):
        """Initialize storage layout.

        Args:
            output_dir: Root output directory
        """
        self.output_dir = Path(output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _validate_ticker(self, ticker: str) -> str:
        """Validate and sanitize ticker to prevent path traversal.

        Args:
            ticker: Issuer ticker

        Returns:
            Validated ticker string

        Raises:
            ValueError: If ticker contains invalid characters
        """
        if not ticker or not VALID_TICKER_PATTERN.match(ticker):
            raise ValueError(
                f"Invalid ticker '{ticker}': must contain only alphanumeric characters, "
                "underscores, and hyphens"
            )
        return ticker

    def _validate_path(self, path: Path) -> Path:
        """Validate that a path is within the output directory.

        Args:
            path: Path to validate

        Returns:
            Resolved path

        Raises:
            ValueError: If path traversal is detected
        """
        resolved = path.resolve()
        if not str(resolved).startswith(str(self.output_dir)):
            raise ValueError(f"Path traversal detected: {path}")
        return resolved

    def get_issuer_dir(self, ticker: str) -> Path:
        """Get the directory for an issuer.

        Args:
            ticker: Issuer ticker

        Returns:
            Path to issuer directory (creates if not exists)

        Raises:
            ValueError: If ticker is invalid or path traversal detected
        """
        ticker = self._validate_ticker(ticker)
        issuer_dir = self._validate_path(self.output_dir / ticker)
        issuer_dir.mkdir(parents=True, exist_ok=True)
        return issuer_dir

    def get_doc_type_dir(self, ticker: str, doc_type: str) -> Path:
        """Get the directory for a document type.

        Args:
            ticker: Issuer ticker
            doc_type: Document type folder name

        Returns:
            Path to document type directory (creates if not exists)
        """
        doc_dir = self.get_issuer_dir(ticker) / doc_type
        doc_dir.mkdir(parents=True, exist_ok=True)
        return doc_dir

    def classify_document(self, filename: str) -> str:
        """Classify a document by its filename.

        Args:
            filename: Document filename

        Returns:
            Document type folder name
        """
        # Remove date prefix if present for matching
        clean_name = filename
        if re.match(r"^\d{8}_", filename):
            clean_name = filename[9:]

        for pattern, doc_type in self.DOC_TYPE_PATTERNS:
            if re.search(pattern, clean_name, re.IGNORECASE):
                return doc_type

        return self.DEFAULT_DOC_TYPE

    def extract_date_from_filename(self, filename: str) -> str | None:
        """Extract date from filename patterns.

        Args:
            filename: Document filename

        Returns:
            Date string (YYYYMMDD) or None if not found
        """
        # Pattern 1: Already has date prefix like 20190603_...
        if re.match(r"^\d{8}_", filename):
            return filename[:8]

        # Pattern 2: ReporteTrimestral_[PERIOD]_[YEAR]_[ISSUER]_...
        match = re.match(r"ReporteTrimestral_(\d+(?:DT|T|D)?)_(\d{4})_", filename)
        if match:
            period, year = match.groups()
            if period in self.QUARTER_DATES:
                return f"{year}{self.QUARTER_DATES[period]}"

        # Pattern 3: ReporteTrimestral_[ISSUER]-[CODE]_[YEAR]_[QUARTER]_...
        match = re.match(r"ReporteTrimestral_[A-Z0-9]+-[A-Z0-9]+_(\d{4})_(\d+[D]?)_", filename)
        if match:
            year, period = match.groups()
            if period in self.QUARTER_DATES:
                return f"{year}{self.QUARTER_DATES[period]}"

        # Pattern 4: ReporteTrimestral_[ISSUER]_[YEAR]_[QUARTER]_...
        match = re.match(r"ReporteTrimestral_[A-Z0-9]+_(\d{4})_(\d+[D]?)_", filename)
        if match:
            year, period = match.groups()
            if period in self.QUARTER_DATES:
                return f"{year}{self.QUARTER_DATES[period]}"

        # Pattern 5: ReporteAnual_[YEAR]_...
        match = re.match(r"ReporteAnual_(\d{4})_", filename)
        if match:
            year = match.group(1)
            return f"{year}1231"

        # Pattern 6: ReporteAnual_[ISSUER]-[CODE]_[YEAR]_...
        match = re.match(r"ReporteAnual_[A-Z0-9]+-[A-Z0-9]+_(\d{4})_", filename)
        if match:
            year = match.group(1)
            return f"{year}1231"

        # Pattern 7: EventoRelevante_[ID]_[ISSUER]_[YEAR]_...
        match = re.match(
            r"EventoRelevante_\d+_[A-Z0-9]+_(\d{4})_(\d+)-(\d{2})(\d{2})(\d{2})-", filename
        )
        if match:
            year, _, yy, mm, dd = match.groups()
            full_year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
            return f"{full_year}{mm}{dd}"

        # Pattern 8: bivaFR2_19_... (assume 2019 for _19_)
        match = re.match(r"bivaFR2_(\d{2})_", filename)
        if match:
            year_short = match.group(1)
            year = f"20{year_short}" if int(year_short) < 50 else f"19{year_short}"
            return f"{year}1231"

        # Pattern 9: Numeric PDF with date like [ID]-YYMMDD-[hash].pdf
        match = re.match(
            r"\d+-(\d{2})(\d{2})(\d{2})-[a-f0-9]+\.(pdf|xbrl|zip)$", filename, re.IGNORECASE
        )
        if match:
            yy, mm, dd = match.groups()[:3]
            year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
            if 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                return f"{year}{mm}{dd}"

        # Pattern 10: PDF with date in name like YYYYMMDD somewhere
        match = re.search(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})", filename)
        if match:
            year, month, day = match.groups()
            if 2000 <= int(year) <= 2030 and 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                return f"{year}{month}{day}"

        return None

    def standardize_filename(self, filename: str, fallback_date: str | None = None) -> str:
        """Standardize a filename with date prefix.

        Args:
            filename: Original filename
            fallback_date: Date to use if none found in filename (YYYYMMDD)

        Returns:
            Standardized filename with date prefix
        """
        # If already has date prefix, just clean up
        if re.match(r"^\d{8}_", filename):
            clean_name = filename[9:]
            clean_name = re.sub(r"%20", "_", clean_name)
            clean_name = re.sub(r"[%\s]+", "_", clean_name)
            clean_name = re.sub(r"_+", "_", clean_name)
            return f"{filename[:8]}_{clean_name}"

        date_prefix = self.extract_date_from_filename(filename)

        if date_prefix is None:
            date_prefix = fallback_date or "00000000"

        # Clean up the filename
        clean_name = filename
        clean_name = re.sub(r"%20", "_", clean_name)
        clean_name = re.sub(r"[%\s]+", "_", clean_name)
        clean_name = re.sub(r"_+", "_", clean_name)

        return f"{date_prefix}_{clean_name}"

    def get_target_path(
        self, ticker: str, filename: str, fallback_date: str | None = None
    ) -> Path:
        """Get the full target path for a document.

        Args:
            ticker: Issuer ticker
            filename: Original filename
            fallback_date: Date to use if none found in filename

        Returns:
            Full path where document should be saved
        """
        doc_type = self.classify_document(filename)
        std_filename = self.standardize_filename(filename, fallback_date)
        return self.get_doc_type_dir(ticker, doc_type) / std_filename

    def list_issuers(self) -> list[str]:
        """List all issuer directories.

        Returns:
            List of issuer ticker strings
        """
        return [
            d.name
            for d in self.output_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

    def list_documents(self, ticker: str, doc_type: str | None = None) -> list[Path]:
        """List all documents for an issuer.

        Args:
            ticker: Issuer ticker
            doc_type: Optional document type filter

        Returns:
            List of document file paths
        """
        issuer_dir = self.get_issuer_dir(ticker)
        documents = []

        if doc_type:
            doc_dir = issuer_dir / doc_type
            if doc_dir.exists():
                documents.extend(f for f in doc_dir.iterdir() if f.is_file())
        else:
            for sub_dir in issuer_dir.iterdir():
                if sub_dir.is_dir():
                    documents.extend(f for f in sub_dir.iterdir() if f.is_file())

        return sorted(documents)
