"""Performance metrics and database-backed analytics."""

import csv
import sqlite3
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from mx_exchange_dataclient.models.xbrl import PerformanceMetrics, ReconciliationAnalysis


class PeriodType(Enum):
    """Supported period types for analysis."""

    YTD = "ytd"  # Year to date
    LTM = "ltm"  # Last twelve months (4 quarters)
    L24M = "l24m"  # Last 24 months (8 quarters)
    ITD = "itd"  # Inception to date
    CUSTOM = "custom"  # Custom date range


def xirr(cashflows: list[tuple[datetime, float]], guess: float = 0.1) -> float | None:
    """Calculate IRR for irregular cashflows using bisection method.

    Args:
        cashflows: List of (date, amount) tuples. Negative = outflow, positive = inflow.
        guess: Initial guess for IRR (not used in bisection, kept for API compatibility).

    Returns:
        Annualized IRR as decimal (e.g., 0.10 for 10%), or None if cannot calculate.
    """
    if not cashflows or len(cashflows) < 2:
        return None

    cashflows = sorted(cashflows, key=lambda x: x[0])
    dates = [cf[0] for cf in cashflows]
    amounts = [cf[1] for cf in cashflows]

    # Need both positive and negative cash flows
    if all(a >= 0 for a in amounts) or all(a <= 0 for a in amounts):
        return None

    years = [(d - dates[0]).days / 365.0 for d in dates]

    def npv(rate: float) -> float:
        if rate <= -1:
            return float("inf")
        return sum(amt / (1 + rate) ** yr for amt, yr in zip(amounts, years))

    # Bisection method
    low, high = -0.99, 5.0

    # Check if solution exists in range
    if npv(low) * npv(high) > 0:
        return None

    mid = (low + high) / 2
    for _ in range(100):
        mid = (low + high) / 2
        if npv(mid) > 0:
            low = mid
        else:
            high = mid
        if abs(high - low) < 1e-6:
            return mid

    return mid


class NAVAnalyticsDB:
    """SQLite database for NAV analytics data."""

    def __init__(self, db_path: str = "nav_analytics.db"):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        """Create database schema."""
        cursor = self.conn.cursor()

        # NAV reconciliation data (quarterly)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nav_reconciliation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                period TEXT NOT NULL,      -- e.g., '2025Q2'
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                balance_date DATE NOT NULL,

                -- Balance sheet items
                nav REAL,
                nav_prior REAL,
                nav_change REAL,
                issued_capital REAL,
                issued_capital_prior REAL,

                -- P&L items
                management_fee REAL DEFAULT 0,
                interest_income REAL DEFAULT 0,
                interest_expense REAL DEFAULT 0,
                net_interest REAL DEFAULT 0,
                realized_gains REAL DEFAULT 0,
                unrealized_gains REAL DEFAULT 0,
                unrealized_losses REAL DEFAULT 0,
                net_unrealized REAL DEFAULT 0,
                fx_gains REAL DEFAULT 0,
                fx_losses REAL DEFAULT 0,
                net_fx REAL DEFAULT 0,
                other_expenses REAL DEFAULT 0,

                -- Capital flows
                capital_calls REAL DEFAULT 0,
                distributions REAL DEFAULT 0,

                -- Reconciliation
                calculated_change REAL,
                reconciliation_diff REAL,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, period)
            )
        """)

        # Cash flows table for IRR calculation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cash_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                flow_date DATE NOT NULL,
                flow_type TEXT NOT NULL,  -- 'capital_call', 'distribution'
                amount REAL NOT NULL,
                period TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, flow_date, flow_type, period)
            )
        """)

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_nav_ticker_date ON nav_reconciliation(ticker, balance_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cf_ticker_date ON cash_flows(ticker, flow_date)"
        )

        self.conn.commit()

    def load_csv(self, ticker: str, csv_path: str):
        """Load NAV reconciliation data from CSV into database.

        Args:
            ticker: Fund ticker (e.g., 'CAPGLPI')
            csv_path: Path to nav_reconciliation.csv file
        """
        cursor = self.conn.cursor()

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse date
                date_str = row["balance_date"]
                try:
                    if "/" in date_str:
                        balance_date = datetime.strptime(date_str, "%d/%m/%y").strftime("%Y-%m-%d")
                    else:
                        balance_date = date_str
                except (ValueError, KeyError):
                    continue

                # Insert reconciliation data
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO nav_reconciliation (
                        ticker, period, year, quarter, balance_date,
                        nav, nav_prior, nav_change,
                        management_fee, interest_income, interest_expense, net_interest,
                        realized_gains, unrealized_gains, unrealized_losses, net_unrealized,
                        fx_gains, fx_losses, net_fx, other_expenses,
                        capital_calls, distributions,
                        calculated_change, reconciliation_diff
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        ticker,
                        row["period"],
                        int(row["year"]),
                        int(row["quarter"]),
                        balance_date,
                        float(row["nav"] or 0),
                        float(row["nav_prior"] or 0) if row.get("nav_prior") else None,
                        float(row["nav_change"] or 0) if row.get("nav_change") else None,
                        float(row.get("management_fee") or 0),
                        float(row.get("interest_income") or 0),
                        float(row.get("interest_expense") or 0),
                        float(row.get("net_interest") or 0),
                        float(row.get("realized_gains") or 0),
                        float(row.get("unrealized_gains") or 0),
                        float(row.get("unrealized_losses") or 0),
                        float(row.get("net_unrealized") or 0),
                        float(row.get("fx_gains") or 0),
                        float(row.get("fx_losses") or 0),
                        float(row.get("net_fx") or 0),
                        float(row.get("other_expenses") or 0),
                        float(row.get("capital_calls") or 0),
                        float(row.get("distributions") or 0),
                        float(row["calculated_change"] or 0) if row.get("calculated_change") else None,
                        float(row["reconciliation_diff"] or 0)
                        if row.get("reconciliation_diff")
                        else None,
                    ),
                )

                # Insert cash flows
                capital_calls = float(row.get("capital_calls") or 0)
                distributions = float(row.get("distributions") or 0)

                if capital_calls > 0:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO cash_flows (ticker, flow_date, flow_type, amount, period)
                        VALUES (?, ?, 'capital_call', ?, ?)
                    """,
                        (ticker, balance_date, -capital_calls, row["period"]),
                    )

                if distributions > 0:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO cash_flows (ticker, flow_date, flow_type, amount, period)
                        VALUES (?, ?, 'distribution', ?, ?)
                    """,
                        (ticker, balance_date, distributions, row["period"]),
                    )

        self.conn.commit()

    def get_date_range(
        self,
        ticker: str,
        period_type: PeriodType,
        as_of_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[str, str]:
        """Calculate start and end dates based on period type.

        Args:
            ticker: Fund ticker
            period_type: PeriodType enum value
            as_of_date: Reference date for relative periods (default: latest available)
            start_date: Required for CUSTOM period type
            end_date: Required for CUSTOM period type

        Returns:
            Tuple of (start_date, end_date) as strings
        """
        cursor = self.conn.cursor()

        # Get reference date
        if as_of_date:
            ref_date = datetime.strptime(as_of_date, "%Y-%m-%d")
        else:
            cursor.execute(
                "SELECT MAX(balance_date) FROM nav_reconciliation WHERE ticker = ?",
                (ticker,),
            )
            max_date = cursor.fetchone()[0]
            ref_date = datetime.strptime(max_date, "%Y-%m-%d")

        if period_type == PeriodType.CUSTOM:
            if not start_date or not end_date:
                raise ValueError("Custom period requires start_date and end_date")
            return start_date, end_date

        elif period_type == PeriodType.ITD:
            cursor.execute(
                "SELECT MIN(balance_date) FROM nav_reconciliation WHERE ticker = ?",
                (ticker,),
            )
            min_date = cursor.fetchone()[0]
            return min_date, ref_date.strftime("%Y-%m-%d")

        elif period_type == PeriodType.YTD:
            start = datetime(ref_date.year, 1, 1)
            cursor.execute(
                """
                SELECT MAX(balance_date) FROM nav_reconciliation
                WHERE ticker = ? AND balance_date < ?
            """,
                (ticker, start.strftime("%Y-%m-%d")),
            )
            prior_date = cursor.fetchone()[0]
            return prior_date or start.strftime("%Y-%m-%d"), ref_date.strftime("%Y-%m-%d")

        elif period_type == PeriodType.LTM:
            start = ref_date - timedelta(days=365)
            cursor.execute(
                """
                SELECT MAX(balance_date) FROM nav_reconciliation
                WHERE ticker = ? AND balance_date <= ?
            """,
                (ticker, start.strftime("%Y-%m-%d")),
            )
            prior_date = cursor.fetchone()[0]
            return prior_date or start.strftime("%Y-%m-%d"), ref_date.strftime("%Y-%m-%d")

        elif period_type == PeriodType.L24M:
            start = ref_date - timedelta(days=730)
            cursor.execute(
                """
                SELECT MAX(balance_date) FROM nav_reconciliation
                WHERE ticker = ? AND balance_date <= ?
            """,
                (ticker, start.strftime("%Y-%m-%d")),
            )
            prior_date = cursor.fetchone()[0]
            return prior_date or start.strftime("%Y-%m-%d"), ref_date.strftime("%Y-%m-%d")

        raise ValueError(f"Unknown period type: {period_type}")

    def get_tickers(self) -> list[str]:
        """Get list of tickers in database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM nav_reconciliation ORDER BY ticker")
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        """Close database connection."""
        self.conn.close()


def performance_metrics(
    db: NAVAnalyticsDB,
    ticker: str,
    period_type: PeriodType = PeriodType.ITD,
    as_of_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> PerformanceMetrics:
    """Calculate performance metrics for a fund.

    Args:
        db: NAVAnalyticsDB instance
        ticker: Fund ticker (e.g., 'CAPGLPI')
        period_type: PeriodType enum value
        as_of_date: Reference date for relative periods (default: latest available)
        start_date: Required for CUSTOM period type
        end_date: Required for CUSTOM period type

    Returns:
        PerformanceMetrics dataclass with all calculated metrics
    """
    cursor = db.conn.cursor()

    # Get date range
    start, end = db.get_date_range(ticker, period_type, as_of_date, start_date, end_date)

    # Get NAV reconciliation data for the period
    cursor.execute(
        """
        SELECT * FROM nav_reconciliation
        WHERE ticker = ? AND balance_date >= ? AND balance_date <= ?
        ORDER BY balance_date
    """,
        (ticker, start, end),
    )

    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f"No data found for {ticker} between {start} and {end}")

    # Calculate aggregates
    total_calls = sum(r["capital_calls"] or 0 for r in rows)
    total_dist = sum(r["distributions"] or 0 for r in rows)
    total_mgmt_fee = sum(r["management_fee"] or 0 for r in rows)
    total_net_interest = sum(r["net_interest"] or 0 for r in rows)
    total_realized = sum(r["realized_gains"] or 0 for r in rows)
    total_unrealized = sum(r["net_unrealized"] or 0 for r in rows)
    total_fx = sum(r["net_fx"] or 0 for r in rows)
    total_other_exp = sum(r["other_expenses"] or 0 for r in rows)

    # Get NAV values
    nav_start = rows[0]["nav_prior"] or rows[0]["nav"]
    nav_end = rows[-1]["nav"]

    # Calculate years
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    years = (end_dt - start_dt).days / 365.0

    # Calculate return metrics
    total_paid_in = total_calls if total_calls > 0 else 1
    tvpi = (total_dist + nav_end) / total_paid_in
    dpi = total_dist / total_paid_in
    rvpi = nav_end / total_paid_in

    # Net P&L
    net_pl = (
        -total_mgmt_fee
        + total_net_interest
        + total_realized
        + total_unrealized
        + total_fx
        - total_other_exp
    )

    # P&L return %
    avg_nav = (nav_start + nav_end) / 2 if nav_start else nav_end / 2
    pl_return_pct = (net_pl / avg_nav * 100) if avg_nav else 0

    # Calculate IRR
    cursor.execute(
        """
        SELECT flow_date, amount FROM cash_flows
        WHERE ticker = ? AND flow_date >= ? AND flow_date <= ?
        ORDER BY flow_date
    """,
        (ticker, start, end),
    )

    cashflows = []
    for row in cursor.fetchall():
        dt = datetime.strptime(row["flow_date"], "%Y-%m-%d")
        cashflows.append((dt, row["amount"]))

    # Add terminal NAV
    if nav_end > 0:
        cashflows.append((end_dt, nav_end))

    irr = xirr(cashflows)

    return PerformanceMetrics(
        ticker=ticker,
        period_type=period_type.value,
        start_date=start,
        end_date=end,
        years=round(years, 2),
        capital_calls=total_calls,
        distributions=total_dist,
        nav_start=nav_start,
        nav_end=nav_end,
        tvpi=round(tvpi, 3),
        dpi=round(dpi, 3),
        rvpi=round(rvpi, 3),
        irr=round(irr, 4) if irr else None,
        management_fee=total_mgmt_fee,
        net_interest=total_net_interest,
        realized_gains=total_realized,
        unrealized_gains=total_unrealized,
        fx_gains=total_fx,
        other_expenses=total_other_exp,
        net_pl=net_pl,
        pl_return_pct=round(pl_return_pct, 2),
    )


def reconciliation_analysis(
    db: NAVAnalyticsDB,
    ticker: str,
    period_type: PeriodType = PeriodType.ITD,
    as_of_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    tolerance_levels: tuple[float, float, float] = (1_000_000, 2_000_000, 5_000_000),
) -> ReconciliationAnalysis:
    """Analyze NAV reconciliation accuracy.

    Args:
        db: NAVAnalyticsDB instance
        ticker: Fund ticker
        period_type: PeriodType enum value
        as_of_date: Reference date for relative periods
        start_date: Required for CUSTOM period type
        end_date: Required for CUSTOM period type
        tolerance_levels: Tuple of (1M, 2M, 5M) tolerance thresholds

    Returns:
        ReconciliationAnalysis dataclass with accuracy metrics
    """
    cursor = db.conn.cursor()

    # Get date range
    start, end = db.get_date_range(ticker, period_type, as_of_date, start_date, end_date)

    # Get reconciliation data
    cursor.execute(
        """
        SELECT * FROM nav_reconciliation
        WHERE ticker = ? AND balance_date >= ? AND balance_date <= ?
        ORDER BY balance_date
    """,
        (ticker, start, end),
    )

    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f"No data found for {ticker} between {start} and {end}")

    # Filter to periods with reconciliation data
    reconciled = [r for r in rows if r["reconciliation_diff"] is not None]

    total_periods = len(rows)
    reconciled_periods = len(reconciled)

    if reconciled_periods == 0:
        raise ValueError(f"No reconciliation data available for {ticker}")

    # Calculate accuracy at different tolerance levels
    tol_1m, tol_2m, tol_5m = tolerance_levels

    within_1m = sum(1 for r in reconciled if abs(r["reconciliation_diff"]) <= tol_1m)
    within_2m = sum(1 for r in reconciled if abs(r["reconciliation_diff"]) <= tol_2m)
    within_5m = sum(1 for r in reconciled if abs(r["reconciliation_diff"]) <= tol_5m)

    # Calculate aggregates
    total_nav_change = sum(r["nav_change"] or 0 for r in reconciled)
    total_calc_change = sum(r["calculated_change"] or 0 for r in reconciled)
    total_diff = sum(r["reconciliation_diff"] or 0 for r in reconciled)

    abs_diffs = [abs(r["reconciliation_diff"]) for r in reconciled]
    avg_abs_diff = sum(abs_diffs) / len(abs_diffs)
    max_abs_diff = max(abs_diffs)

    # Identify problem periods
    problem_periods: list[dict[str, Any]] = []
    for r in reconciled:
        if abs(r["reconciliation_diff"]) > tol_2m:
            problem_periods.append(
                {
                    "period": r["period"],
                    "balance_date": r["balance_date"],
                    "nav_change": r["nav_change"],
                    "calculated_change": r["calculated_change"],
                    "difference": r["reconciliation_diff"],
                }
            )

    return ReconciliationAnalysis(
        ticker=ticker,
        period_type=period_type.value,
        start_date=start,
        end_date=end,
        total_periods=total_periods,
        reconciled_periods=reconciled_periods,
        periods_within_1m=within_1m,
        periods_within_2m=within_2m,
        periods_within_5m=within_5m,
        accuracy_1m_pct=round(within_1m / reconciled_periods * 100, 1),
        accuracy_2m_pct=round(within_2m / reconciled_periods * 100, 1),
        accuracy_5m_pct=round(within_5m / reconciled_periods * 100, 1),
        total_nav_change=total_nav_change,
        total_calculated_change=total_calc_change,
        total_difference=total_diff,
        avg_abs_difference=round(avg_abs_diff, 0),
        max_abs_difference=max_abs_diff,
        problem_periods=problem_periods,
    )


def compare_funds(
    db: NAVAnalyticsDB,
    tickers: list[str],
    period_type: PeriodType = PeriodType.ITD,
    as_of_date: str | None = None,
) -> tuple[list[PerformanceMetrics], list[ReconciliationAnalysis]]:
    """Compare multiple funds.

    Args:
        db: NAVAnalyticsDB instance
        tickers: List of fund tickers
        period_type: PeriodType enum value
        as_of_date: Reference date

    Returns:
        Tuple of (performance_metrics_list, reconciliation_analysis_list)
    """
    metrics = []
    analyses = []

    for ticker in tickers:
        try:
            m = performance_metrics(db, ticker, period_type, as_of_date)
            metrics.append(m)

            a = reconciliation_analysis(db, ticker, period_type, as_of_date)
            analyses.append(a)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    return metrics, analyses
