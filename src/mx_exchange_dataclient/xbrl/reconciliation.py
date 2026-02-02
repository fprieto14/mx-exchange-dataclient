"""NAV reconciliation functions."""

import re
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from mx_exchange_dataclient.models.xbrl import (
    NAVReconciliation,
    NAVReconciliationReport,
    XBRLData,
)
from mx_exchange_dataclient.xbrl.parser import XBRLParser


class ReportPeriod(Enum):
    """Period types for NAV reconciliation reports."""

    QUARTERLY = "quarterly"  # Single quarter transition
    YTD = "ytd"  # Year to date
    LTM = "ltm"  # Last twelve months (4 quarters)
    ANNUAL = "annual"  # Full fiscal year
    ITD = "itd"  # Inception to date


def find_xbrl_files(folder: str | Path) -> list[dict[str, Any]]:
    """Scan folder for XBRL files and extract period metadata.

    Args:
        folder: Path to folder containing XBRL files

    Returns:
        List of dicts with 'path', 'year', 'quarter', 'sort_key' for each file
        Deduplicates by period, keeping the file with the latest timestamp.
    """
    folder_path = Path(folder)
    files_by_period: dict[str, dict[str, Any]] = {}  # period -> file info (deduplicate)

    for f in folder_path.glob("*.xbrl"):
        fname = f.name
        # Skip macOS metadata files
        if fname.startswith("._"):
            continue
        # Parse period from filename: ReporteTrimestral_1T_2025_...
        match = re.search(r"_([1-4])T_(\d{4})_", fname)
        if match:
            quarter = int(match.group(1))
            year = int(match.group(2))
            # Sort key: year * 10 + quarter (e.g., 20251 for Q1 2025)
            sort_key = year * 10 + quarter
            period = f"{quarter}T_{year}"

            # Extract timestamp from filename (usually at the end before .xbrl)
            ts_match = re.search(r"_(\d{10,})\.xbrl$", fname)
            timestamp = int(ts_match.group(1)) if ts_match else 0

            file_info = {
                "path": str(f),
                "year": year,
                "quarter": quarter,
                "sort_key": sort_key,
                "period": period,
                "timestamp": timestamp,
            }

            # Keep file with latest timestamp for each period
            if period not in files_by_period or timestamp > files_by_period[period]["timestamp"]:
                files_by_period[period] = file_info

    return sorted(files_by_period.values(), key=lambda x: x["sort_key"])


def select_files_for_period(
    files: list[dict[str, Any]],
    period_type: ReportPeriod,
    as_of_year: int | None = None,
    as_of_quarter: int | None = None,
) -> list[dict[str, Any]]:
    """Select appropriate files based on period type.

    Args:
        files: List of file metadata from find_xbrl_files()
        period_type: ReportPeriod enum value
        as_of_year: Reference year (default: latest available)
        as_of_quarter: Reference quarter (default: latest available)

    Returns:
        List of selected file metadata, sorted chronologically
    """
    if not files:
        return []

    # Default to latest available period
    if as_of_year is None:
        as_of_year = files[-1]["year"]
    if as_of_quarter is None:
        as_of_quarter = files[-1]["quarter"]

    as_of_key = as_of_year * 10 + as_of_quarter

    # Filter to files up to as_of date
    available = [f for f in files if f["sort_key"] <= as_of_key]
    if not available:
        return []

    if period_type == ReportPeriod.QUARTERLY:
        # Last 2 periods for single quarter transition
        return available[-2:] if len(available) >= 2 else available

    elif period_type == ReportPeriod.YTD:
        # From Q4 of prior year through current quarter
        prior_q4_key = (as_of_year - 1) * 10 + 4
        selected = [f for f in available if f["sort_key"] >= prior_q4_key]
        return selected if selected else available[-5:]  # Fallback to last 5

    elif period_type == ReportPeriod.LTM:
        # Last 5 periods (4 quarters of change = 5 data points)
        return available[-5:] if len(available) >= 5 else available

    elif period_type == ReportPeriod.ANNUAL:
        # Q4 prior year through Q4 current year (or latest)
        # Find Q4 of prior year as starting point
        if as_of_quarter == 4:
            # Full year: Q4 (year-1) to Q4 (year)
            start_key = (as_of_year - 1) * 10 + 4
            end_key = as_of_year * 10 + 4
        else:
            # Incomplete year: Q4 (year-1) to latest
            start_key = (as_of_year - 1) * 10 + 4
            end_key = as_of_key
        selected = [f for f in available if start_key <= f["sort_key"] <= end_key]
        return selected if selected else available[-5:]

    elif period_type == ReportPeriod.ITD:
        # All available data
        return available

    return available


def nav_reconciliation(
    data: list[XBRLData],
    ticker: str = "FUND",
) -> NAVReconciliationReport:
    """Calculate NAV reconciliation across multiple periods.

    The reconciliation verifies two fundamental accounting identities:
    1. NAV = Assets - Liabilities (Asset decomposition)
    2. Equity = IssuedCapital + RetainedEarnings (Equity decomposition)

    Args:
        data: List of XBRLData objects sorted chronologically
        ticker: Fund ticker for the report

    Returns:
        NAVReconciliationReport with quarterly and LTM reconciliation
    """
    quarterly_recons = []

    for i in range(1, len(data)):
        prev = data[i - 1]
        curr = data[i]

        # NAV (Equity) changes
        nav_open = prev.balance_sheet.get("equity", 0)
        nav_close = curr.balance_sheet.get("equity", 0)
        nav_change = nav_close - nav_open

        # Asset decomposition
        cash_open = prev.balance_sheet.get("cash", 0)
        cash_close = curr.balance_sheet.get("cash", 0)
        cash_change = cash_close - cash_open

        other_assets_open = prev.balance_sheet.get("assets", 0) - cash_open
        other_assets_close = curr.balance_sheet.get("assets", 0) - cash_close
        other_assets_change = other_assets_close - other_assets_open

        liab_open = prev.balance_sheet.get("liabilities", 0)
        liab_close = curr.balance_sheet.get("liabilities", 0)
        liab_change = liab_close - liab_open

        # Calculated NAV change from asset decomposition
        calculated_change = cash_change + other_assets_change - liab_change

        # Equity decomposition (from balance sheet)
        capital_open = prev.balance_sheet.get("issued_capital", 0)
        capital_close = curr.balance_sheet.get("issued_capital", 0)
        capital_change = capital_close - capital_open

        retained_open = prev.balance_sheet.get("retained_earnings", 0)
        retained_close = curr.balance_sheet.get("retained_earnings", 0)
        retained_change = retained_close - retained_open

        # P&L from income statement (quarterly, not YTD)
        quarterly_pl = curr.pl.get("profit_loss", 0)
        ytd_pl = curr.pl_ytd.get("profit_loss", 0) if curr.pl_ytd else 0
        dividends = curr.pl.get("dividends_paid", 0)

        # Gaps
        retained_vs_pl_gap = retained_change - quarterly_pl  # Prior period adjustments
        equity_recon_gap = nav_change - (capital_change + retained_change)

        quarterly_recons.append(
            NAVReconciliation(
                period_from=prev.period,
                period_to=curr.period,
                nav_open=nav_open,
                nav_close=nav_close,
                nav_change=nav_change,
                cash_open=cash_open,
                cash_close=cash_close,
                cash_change=cash_change,
                other_assets_open=other_assets_open,
                other_assets_close=other_assets_close,
                other_assets_change=other_assets_change,
                liabilities_open=liab_open,
                liabilities_close=liab_close,
                liabilities_change=liab_change,
                capital_open=capital_open,
                capital_close=capital_close,
                capital_change=capital_change,
                retained_earnings_open=retained_open,
                retained_earnings_close=retained_close,
                retained_earnings_change=retained_change,
                profit_loss_quarterly=quarterly_pl,
                profit_loss_ytd=ytd_pl,
                dividends=dividends,
                retained_vs_pl_gap=retained_vs_pl_gap,
                equity_reconciliation_gap=equity_recon_gap,
                calculated_nav_change=calculated_change,
            )
        )

    # LTM aggregates
    first = data[0]
    last = data[-1]

    ltm_nav_open = first.balance_sheet.get("equity", 0)
    ltm_nav_close = last.balance_sheet.get("equity", 0)
    ltm_cash_change = last.balance_sheet.get("cash", 0) - first.balance_sheet.get("cash", 0)
    ltm_other_assets_change = (last.balance_sheet.get("assets", 0) - last.balance_sheet.get("cash", 0)) - (
        first.balance_sheet.get("assets", 0) - first.balance_sheet.get("cash", 0)
    )
    ltm_liab_change = last.balance_sheet.get("liabilities", 0) - first.balance_sheet.get(
        "liabilities", 0
    )
    ltm_capital_change = last.balance_sheet.get("issued_capital", 0) - first.balance_sheet.get(
        "issued_capital", 0
    )
    ltm_retained_change = last.balance_sheet.get("retained_earnings", 0) - first.balance_sheet.get(
        "retained_earnings", 0
    )
    ltm_pl = sum(d.pl.get("profit_loss", 0) for d in data[1:])
    ltm_div = sum(d.pl.get("dividends_paid", 0) for d in data[1:])
    ltm_prior_adj = ltm_retained_change - ltm_pl

    return NAVReconciliationReport(
        ticker=ticker,
        periods=data,
        quarterly_reconciliations=quarterly_recons,
        ltm_nav_open=ltm_nav_open,
        ltm_nav_close=ltm_nav_close,
        ltm_nav_change=ltm_nav_close - ltm_nav_open,
        ltm_cash_change=ltm_cash_change,
        ltm_other_assets_change=ltm_other_assets_change,
        ltm_liabilities_change=ltm_liab_change,
        ltm_capital_change=ltm_capital_change,
        ltm_retained_earnings_change=ltm_retained_change,
        ltm_profit_loss=ltm_pl,
        ltm_dividends=ltm_div,
        ltm_prior_period_adj=ltm_prior_adj,
    )


def nav_reconciliation_by_period(
    ticker: str,
    xbrl_folder: str | Path,
    period_type: ReportPeriod | str = ReportPeriod.LTM,
    as_of: str | None = None,
) -> NAVReconciliationReport:
    """Run NAV reconciliation for a specific period type.

    Args:
        ticker: Fund ticker (e.g., 'AYLLUPI')
        xbrl_folder: Path to folder containing XBRL files
        period_type: ReportPeriod enum or string ('quarterly', 'ytd', 'ltm', 'annual', 'itd')
        as_of: Reference date as 'YYYY-MM-DD' or 'YYYY-QN' (e.g., '2025-Q3')
               Default: latest available period

    Returns:
        NAVReconciliationReport with reconciliation for the specified period

    Example:
        report = nav_reconciliation_by_period(
            ticker="AYLLUPI",
            xbrl_folder="/Volumes/Fernando/output/AYLLUPI/xbrls",
            period_type="annual",
            as_of="2025-Q3"
        )
        report.print_report()
    """
    # Convert string to enum if needed
    if isinstance(period_type, str):
        period_type = ReportPeriod(period_type.lower())

    # Parse as_of date
    as_of_year = None
    as_of_quarter = None
    if as_of:
        if "-Q" in as_of.upper():
            # Format: 2025-Q3
            parts = as_of.upper().split("-Q")
            as_of_year = int(parts[0])
            as_of_quarter = int(parts[1])
        else:
            # Format: 2025-09-30 - derive quarter from month
            dt = datetime.strptime(as_of, "%Y-%m-%d")
            as_of_year = dt.year
            as_of_quarter = (dt.month - 1) // 3 + 1

    # Find and select files
    all_files = find_xbrl_files(xbrl_folder)
    if not all_files:
        raise ValueError(f"No XBRL files found in {xbrl_folder}")

    selected = select_files_for_period(all_files, period_type, as_of_year, as_of_quarter)
    if len(selected) < 2:
        raise ValueError(f"Need at least 2 periods for reconciliation, found {len(selected)}")

    # Parse XBRL files
    parser = XBRLParser()
    data = [parser.parse(f["path"]) for f in selected]

    # Run reconciliation
    return nav_reconciliation(data, ticker=ticker)
