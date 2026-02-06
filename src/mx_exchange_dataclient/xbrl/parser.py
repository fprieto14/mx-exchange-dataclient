"""XBRL file parser for Mexican financial instruments."""

import html
import re
from datetime import datetime
from pathlib import Path

import defusedxml.ElementTree as ET

from mx_exchange_dataclient.models.xbrl import XBRLData
from mx_exchange_dataclient.xbrl.concepts import BALANCE_SHEET_CONCEPTS, PL_CONCEPTS

# XBRL element local names for audit metadata (mx_ccd namespace)
_AUDIT_OPINION_TAG = "TypeOfOpinionOnTheFinancialStatements"
_AUDIT_DATE_TAG = "DateOfOpinionOnTheFinancialStatements"
_AUDITOR_FIRM_TAG = "NameServiceProviderExternalAudit"

# Keywords to classify audit opinion text into standard categories
_OPINION_KEYWORDS: dict[str, list[str]] = {
    "limpio": [
        "limpia", "limpio", "sin salvedad", "sin salvedades", "favorable",
        "unqualified", "clean", "positiva", "positivo",
        "presentan razonablemente", "presentan razonable",
        "presentan en forma razonable",
    ],
    "con_salvedad": ["con salvedad", "con salvedades", "qualified", "except for"],
    "negativa": ["negativa", "adverse", "desfavorable"],
    "abstencion": ["abstención", "abstencion", "disclaimer", "denegación", "denegacion"],
}


class XBRLParser:
    """Parse XBRL files and extract balance sheet + P&L data."""

    def __init__(
        self,
        balance_sheet_concepts: dict[str, str] | None = None,
        pl_concepts: dict[str, str] | None = None,
    ):
        """Initialize parser with concept mappings.

        Args:
            balance_sheet_concepts: Optional custom balance sheet concept mapping
            pl_concepts: Optional custom P&L concept mapping
        """
        self.balance_sheet_concepts = balance_sheet_concepts or BALANCE_SHEET_CONCEPTS
        self.pl_concepts = pl_concepts or PL_CONCEPTS

    def parse(self, filepath: str | Path) -> XBRLData:
        """Parse an XBRL file and extract financial data.

        Args:
            filepath: Path to XBRL file

        Returns:
            XBRLData with extracted balance sheet and P&L values
        """
        filepath = Path(filepath)
        tree = ET.parse(filepath)
        root = tree.getroot()

        # Find contexts with detailed period info
        contexts = {}
        for elem in root.iter():
            if "context" in elem.tag.lower() and elem.tag.endswith("}context"):
                ctx_id = elem.get("id")
                ctx_info: dict = {"type": None}
                for child in elem.iter():
                    if "instant" in child.tag:
                        ctx_info["type"] = "instant"
                        ctx_info["date"] = child.text
                    elif "startDate" in child.tag:
                        ctx_info["type"] = "duration"
                        ctx_info["start"] = child.text
                    elif "endDate" in child.tag:
                        ctx_info["end"] = child.text
                if ctx_info["type"]:
                    if (
                        ctx_info["type"] == "duration"
                        and "start" in ctx_info
                        and "end" in ctx_info
                    ):
                        start = datetime.strptime(ctx_info["start"], "%Y-%m-%d")
                        end = datetime.strptime(ctx_info["end"], "%Y-%m-%d")
                        ctx_info["days"] = (end - start).days
                    contexts[ctx_id] = ctx_info

        # Extract balance sheet values (instant contexts)
        balance_sheet: dict[str, tuple[float, str]] = {}
        for elem in root.iter():
            tag = elem.tag
            ctx_ref = elem.get("contextRef")

            if not ctx_ref or ctx_ref not in contexts:
                continue

            ctx = contexts[ctx_ref]
            if ctx["type"] != "instant":
                continue

            if tag in self.balance_sheet_concepts:
                key = self.balance_sheet_concepts[tag]
                try:
                    val = float(elem.text)
                    date = ctx["date"]
                    # Keep most recent date for each concept, prefer larger values for equity
                    if key not in balance_sheet:
                        balance_sheet[key] = (val, date)
                    elif date > balance_sheet[key][1]:
                        balance_sheet[key] = (val, date)
                    elif (
                        date == balance_sheet[key][1]
                        and key == "equity"
                        and val > balance_sheet[key][0]
                    ):
                        balance_sheet[key] = (val, date)
                except (ValueError, TypeError):
                    pass

        # Extract P&L values - separate quarterly (80-100 days) from YTD (>200 days)
        pl_quarterly: dict[str, tuple[float, str]] = {}
        pl_ytd: dict[str, tuple[float, str]] = {}

        for elem in root.iter():
            tag = elem.tag
            ctx_ref = elem.get("contextRef")

            if not ctx_ref or ctx_ref not in contexts:
                continue

            ctx = contexts[ctx_ref]
            if ctx["type"] != "duration":
                continue

            if tag in self.pl_concepts:
                key = self.pl_concepts[tag]
                try:
                    val = float(elem.text)
                    days = ctx.get("days", 0)

                    # Quarterly context (around 90 days)
                    if 80 <= days <= 100:
                        if key not in pl_quarterly or val > pl_quarterly[key][0]:
                            pl_quarterly[key] = (val, f"{ctx['start']} to {ctx['end']}")

                    # YTD context (> 200 days)
                    elif days > 200:
                        if key not in pl_ytd or val > pl_ytd[key][0]:
                            pl_ytd[key] = (val, f"{ctx['start']} to {ctx['end']}")

                except (ValueError, TypeError):
                    pass

        # Extract period from filename
        period = self._extract_period(str(filepath))

        return XBRLData(
            period=period,
            file=filepath.name,
            balance_sheet={k: v[0] for k, v in balance_sheet.items()},
            balance_dates={k: v[1] for k, v in balance_sheet.items()},
            pl={k: v[0] for k, v in pl_quarterly.items()},
            pl_ytd={k: v[0] for k, v in pl_ytd.items()},
        )

    def _extract_period(self, filepath: str) -> str:
        """Extract period identifier from filename."""
        fname = Path(filepath).stem
        patterns = [
            (r"_([1-4]T)_(\d{4})", lambda m: f"{m.group(1)}_{m.group(2)}"),
            (r"_([1-4]Q)_(\d{4})", lambda m: f"{m.group(1)}_{m.group(2)}"),
            (r"_(4DT)_(\d{4})", lambda m: f"{m.group(1)}_{m.group(2)}"),
        ]
        for pattern, formatter in patterns:
            match = re.search(pattern, fname)
            if match:
                return formatter(match)
        return "Unknown"

    def extract_audit_metadata(self, filepath: str | Path) -> dict[str, str | None]:
        """Extract audit opinion metadata from an XBRL file (4DT annual filings).

        Args:
            filepath: Path to XBRL file

        Returns:
            Dict with 'audit_opinion', 'auditor_firm', 'opinion_date' keys.
            Values are None if the element is not found.
        """
        filepath = Path(filepath)
        tree = ET.parse(filepath)
        root = tree.getroot()

        result: dict[str, str | None] = {
            "audit_opinion": None,
            "auditor_firm": None,
            "opinion_date": None,
        }

        opinion_element_found = False

        for elem in root.iter():
            local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            text = (elem.text or "").strip()

            if local == _AUDIT_OPINION_TAG:
                opinion_element_found = True
                if text:
                    decoded = html.unescape(text)
                    clean = re.sub(r"<[^>]+>", " ", decoded).strip()
                    clean = re.sub(r"\s+", " ", clean)
                    if clean:
                        result["audit_opinion"] = _classify_opinion(clean)
            elif local == _AUDITOR_FIRM_TAG and text:
                result["auditor_firm"] = html.unescape(text).strip()
            elif local == _AUDIT_DATE_TAG and text:
                result["opinion_date"] = html.unescape(text).strip()

        # Distinguish: element absent vs element present but empty/unclassified
        if result["audit_opinion"] is None:
            if opinion_element_found:
                result["audit_opinion"] = "element_empty"
            else:
                result["audit_opinion"] = "not_found"

        return result

    def parse_multiple(self, filepaths: list[str | Path]) -> list[XBRLData]:
        """Parse multiple XBRL files.

        Args:
            filepaths: List of paths to XBRL files

        Returns:
            List of XBRLData objects
        """
        return [self.parse(f) for f in filepaths]


def _classify_opinion(text: str) -> str:
    """Classify raw audit opinion text into a standard category.

    Returns one of: 'limpio', 'con_salvedad', 'negativa', 'abstencion'.
    Falls back to the raw text (truncated to 50 chars) if no keyword matches.
    """
    lower = text.lower()
    # Check limpio FIRST — "sin salvedad(es)" must take priority over
    # "con salvedad(es)" which may appear later in the same text as a
    # negated enumeration (e.g. "no ha emitido opinión con salvedades").
    for keyword in _OPINION_KEYWORDS["limpio"]:
        if keyword in lower:
            return "limpio"
    for category in ("con_salvedad", "negativa", "abstencion"):
        for keyword in _OPINION_KEYWORDS[category]:
            if keyword in lower:
                return category
    # Fallback: prefix with "unclassified:" so it's distinguishable from not_found
    return f"unclassified:{text[:37]}" if text else ""
