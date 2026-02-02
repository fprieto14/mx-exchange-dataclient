"""Data loading utilities for XBRL mappings and issuer registries.

This module provides functions to load bundled data files:
- XBRL concept mappings
- XBRL taxonomy concepts
- Known issuer registries (BIVA + BMV)
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

# Package data directory
DATA_DIR = Path(__file__).parent


@lru_cache(maxsize=1)
def load_xbrl_mappings() -> dict[str, Any]:
    """Load XBRL mappings from bundled JSON file.

    Returns:
        Dict with issuer-specific concept mappings

    Example:
        >>> mappings = load_xbrl_mappings()
        >>> capglpi = mappings.get("CAPGLPI", {})
        >>> nav_concept = capglpi.get("mappings", {}).get("nav")
    """
    mappings_file = DATA_DIR / "xbrl_mappings.json"
    if not mappings_file.exists():
        return {}

    with open(mappings_file, "r") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def load_known_issuers() -> dict[str, dict[str, Any]]:
    """Load combined known issuers registry.

    Returns:
        Dict mapping ticker -> issuer info

    Example:
        >>> issuers = load_known_issuers()
        >>> capglpi = issuers["CAPGLPI"]
        >>> print(capglpi["id"], capglpi["source"])  # 2215, 'biva'
    """
    issuers_file = DATA_DIR / "known_issuers.json"
    if issuers_file.exists():
        with open(issuers_file, "r") as f:
            return json.load(f)

    # Fall back to hardcoded registries
    return {
        # BIVA issuers
        "CAPGLPI": {"id": 2215, "source": "biva", "name": "Capital Global PI"},
        "QTZALPI": {"id": 2282, "source": "biva", "name": "Quetzal PI"},
        # BMV issuers
        "LOCKXPI": {
            "id": 35563,
            "source": "bmv",
            "market": "CGEN_CAPIT",
            "name": "Lock Capital Private Investment I CKD",
        },
    }


def get_issuer_info(ticker: str) -> dict[str, Any] | None:
    """Get info for a specific issuer.

    Args:
        ticker: Issuer ticker (case-insensitive)

    Returns:
        Dict with issuer info or None if not found
    """
    issuers = load_known_issuers()
    return issuers.get(ticker.upper())


def get_issuer_mapping(ticker: str) -> dict[str, Any] | None:
    """Get XBRL mappings for a specific issuer.

    Args:
        ticker: Issuer ticker (case-insensitive)

    Returns:
        Dict with XBRL concept mappings or None if not found
    """
    mappings = load_xbrl_mappings()
    return mappings.get(ticker.upper())


def get_concept_for_issuer(ticker: str, concept_key: str) -> dict[str, Any] | None:
    """Get a specific concept mapping for an issuer.

    Args:
        ticker: Issuer ticker
        concept_key: Concept key (e.g., 'nav', 'issued_capital')

    Returns:
        Dict with concept info or None if not found
    """
    issuer_mappings = get_issuer_mapping(ticker)
    if not issuer_mappings:
        return None

    mappings = issuer_mappings.get("mappings", {})
    return mappings.get(concept_key)


def load_taxonomy_concepts() -> list[str]:
    """Load XBRL taxonomy concepts from text file.

    Returns:
        List of concept names
    """
    concepts_file = DATA_DIR / "xbrl_taxonomy_concepts.txt"
    if not concepts_file.exists():
        return []

    with open(concepts_file, "r") as f:
        return [line.strip() for line in f if line.strip()]


def list_available_issuers() -> list[str]:
    """List all known issuer tickers.

    Returns:
        Sorted list of ticker strings
    """
    return sorted(load_known_issuers().keys())


def list_biva_issuers() -> list[str]:
    """List BIVA issuer tickers.

    Returns:
        List of BIVA ticker strings
    """
    issuers = load_known_issuers()
    return [k for k, v in issuers.items() if v.get("source") == "biva"]


def list_bmv_issuers() -> list[str]:
    """List BMV issuer tickers.

    Returns:
        List of BMV ticker strings
    """
    issuers = load_known_issuers()
    return [k for k, v in issuers.items() if v.get("source") == "bmv"]


# Re-export for convenience
__all__ = [
    "load_xbrl_mappings",
    "load_known_issuers",
    "get_issuer_info",
    "get_issuer_mapping",
    "get_concept_for_issuer",
    "load_taxonomy_concepts",
    "list_available_issuers",
    "list_biva_issuers",
    "list_bmv_issuers",
    "DATA_DIR",
]
