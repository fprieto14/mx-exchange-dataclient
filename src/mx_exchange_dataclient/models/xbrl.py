"""Pydantic and dataclass models for XBRL data and NAV analytics."""

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class XBRLData:
    """Parsed XBRL data for a single reporting period."""

    period: str  # e.g., '1T_2025'
    file: str  # Source filename
    balance_sheet: dict[str, float]  # Concept -> value
    balance_dates: dict[str, str]  # Concept -> date
    pl: dict[str, float]  # P&L concept -> value (quarterly)
    pl_ytd: dict[str, float] = field(default_factory=dict)  # P&L YTD values
    audit_opinion: str | None = None  # "limpio", "con_salvedad", "negativa", "abstencion"
    auditor_firm: str | None = None  # e.g., "KPMG"
    opinion_date: str | None = None  # e.g., "27 de junio de 2024"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NAVReconciliation:
    """NAV reconciliation for a single quarter transition."""

    period_from: str
    period_to: str

    # Opening/Closing NAV
    nav_open: float
    nav_close: float
    nav_change: float

    # Asset decomposition
    cash_open: float
    cash_close: float
    cash_change: float
    other_assets_open: float
    other_assets_close: float
    other_assets_change: float

    # Liability changes
    liabilities_open: float
    liabilities_close: float
    liabilities_change: float

    # Equity decomposition (from balance sheet)
    capital_open: float
    capital_close: float
    capital_change: float  # ΔIssuedCapital
    retained_earnings_open: float
    retained_earnings_close: float
    retained_earnings_change: float  # ΔRetainedEarnings

    # P&L from income statement
    profit_loss_quarterly: float  # Quarterly P&L (90-day context)
    profit_loss_ytd: float  # YTD P&L for reference
    dividends: float

    # Reconciliation gaps
    retained_vs_pl_gap: float  # ΔRetainedEarnings - Q P&L (prior period adj)
    equity_reconciliation_gap: float  # ΔEquity - (ΔCapital + ΔRetained)

    # Calculated NAV change from asset decomposition
    calculated_nav_change: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NAVReconciliationReport:
    """Complete NAV reconciliation report for multiple periods."""

    ticker: str
    periods: list[XBRLData]
    quarterly_reconciliations: list[NAVReconciliation]

    # LTM/ITD aggregates
    ltm_nav_open: float
    ltm_nav_close: float
    ltm_nav_change: float
    ltm_cash_change: float
    ltm_other_assets_change: float
    ltm_liabilities_change: float
    ltm_capital_change: float
    ltm_retained_earnings_change: float
    ltm_profit_loss: float
    ltm_dividends: float
    ltm_prior_period_adj: float  # Sum of ΔRetained - Q P&L

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "periods": [p.to_dict() for p in self.periods],
            "quarterly_reconciliations": [r.to_dict() for r in self.quarterly_reconciliations],
            "ltm_nav_open": self.ltm_nav_open,
            "ltm_nav_close": self.ltm_nav_close,
            "ltm_nav_change": self.ltm_nav_change,
            "ltm_cash_change": self.ltm_cash_change,
            "ltm_other_assets_change": self.ltm_other_assets_change,
            "ltm_liabilities_change": self.ltm_liabilities_change,
            "ltm_capital_change": self.ltm_capital_change,
            "ltm_retained_earnings_change": self.ltm_retained_earnings_change,
            "ltm_profit_loss": self.ltm_profit_loss,
            "ltm_dividends": self.ltm_dividends,
            "ltm_prior_period_adj": self.ltm_prior_period_adj,
        }

    def print_report(self):
        """Print formatted NAV reconciliation report."""
        print("=" * 100)
        print(f"{self.ticker} - BALANCE SHEET EVOLUTION")
        print("=" * 100)
        print(f"{'Concept':<30} ", end="")
        for p in self.periods:
            print(f"{p.period:>15}", end="")
        print()
        print("-" * 100)

        for concept in [
            "equity",
            "issued_capital",
            "retained_earnings",
            "assets",
            "cash",
            "investments",
            "liabilities",
        ]:
            print(f"{concept:<30} ", end="")
            for p in self.periods:
                val = p.balance_sheet.get(concept, 0)
                if val:
                    print(f"{val/1e6:>15,.1f}", end="")
                else:
                    print(f"{'N/A':>15}", end="")
            print()

        print("\n" + "=" * 100)
        print("NAV RECONCILIATION - Quarter over Quarter")
        print("=" * 100)

        for r in self.quarterly_reconciliations:
            print(f"\n{r.period_from} → {r.period_to}")
            print("-" * 70)
            print(f"{'NAV Opening (Equity):':<45} {r.nav_open/1e6:>15,.2f} M")
            print()
            print("ASSET DECOMPOSITION: (verifies NAV = Assets - Liabilities)")
            print(f"  {'+ ΔCash & Equivalents:':<43} {r.cash_change/1e6:>15,.2f} M")
            print(f"  {'+ ΔOther Assets (Investments):':<43} {r.other_assets_change/1e6:>15,.2f} M")
            print(f"  {'- ΔLiabilities:':<43} {-r.liabilities_change/1e6:>15,.2f} M")
            print(f"  {'= Calculated ΔNAV:':<43} {r.calculated_nav_change/1e6:>15,.2f} M")
            print()
            print("EQUITY DECOMPOSITION: (verifies Equity = Capital + Retained)")
            print(f"  {'+ ΔIssuedCapital (net contributions):':<43} {r.capital_change/1e6:>15,.2f} M")
            print(f"  {'+ ΔRetainedEarnings:':<43} {r.retained_earnings_change/1e6:>15,.2f} M")
            print(
                f"      {'└─ Quarterly P&L:':<41} {r.profit_loss_quarterly/1e6:>15,.2f} M"
            )
            if abs(r.retained_vs_pl_gap) > 0.01:
                print(
                    f"      {'└─ Prior period adj:':<41} {r.retained_vs_pl_gap/1e6:>15,.2f} M"
                )
            print(
                f"  {'= Calculated ΔNAV:':<43} {(r.capital_change + r.retained_earnings_change)/1e6:>15,.2f} M"
            )
            print()
            print(f"{'NAV Closing (Equity):':<45} {r.nav_close/1e6:>15,.2f} M")
            print(f"{'Actual ΔNAV:':<45} {r.nav_change/1e6:>15,.2f} M")
            if abs(r.equity_reconciliation_gap) > 0.01:
                print(
                    f"{'Reconciliation Gap:':<45} {r.equity_reconciliation_gap/1e6:>15,.2f} M"
                )

        print("\n" + "=" * 100)
        print("LTM CONSOLIDATED")
        print("=" * 100)
        print(f"{'NAV Opening:':<45} {self.ltm_nav_open/1e6:>15,.2f} M")
        print()
        print("LTM ASSET DECOMPOSITION:")
        print(f"  {'+ ΔCash & Equivalents:':<43} {self.ltm_cash_change/1e6:>15,.2f} M")
        print(
            f"  {'+ ΔOther Assets (Investments):':<43} {self.ltm_other_assets_change/1e6:>15,.2f} M"
        )
        print(f"  {'- ΔLiabilities:':<43} {-self.ltm_liabilities_change/1e6:>15,.2f} M")
        calc_ltm = (
            self.ltm_cash_change + self.ltm_other_assets_change - self.ltm_liabilities_change
        )
        print(f"  {'= Calculated ΔNAV:':<43} {calc_ltm/1e6:>15,.2f} M")
        print()
        print("LTM EQUITY DECOMPOSITION:")
        print(
            f"  {'+ ΔIssuedCapital (net contributions):':<43} {self.ltm_capital_change/1e6:>15,.2f} M"
        )
        print(
            f"  {'+ ΔRetainedEarnings:':<43} {self.ltm_retained_earnings_change/1e6:>15,.2f} M"
        )
        print(f"      {'└─ LTM Quarterly P&L:':<41} {self.ltm_profit_loss/1e6:>15,.2f} M")
        if abs(self.ltm_prior_period_adj) > 0.01:
            print(
                f"      {'└─ Prior period adj:':<41} {self.ltm_prior_period_adj/1e6:>15,.2f} M"
            )
        print(f"  {'- LTM Dividends:':<43} {-self.ltm_dividends/1e6:>15,.2f} M")
        calc_equity = self.ltm_capital_change + self.ltm_retained_earnings_change
        print(f"  {'= Calculated ΔNAV:':<43} {calc_equity/1e6:>15,.2f} M")
        print()
        print(f"{'NAV Closing:':<45} {self.ltm_nav_close/1e6:>15,.2f} M")
        print(f"{'LTM ΔNAV:':<45} {self.ltm_nav_change/1e6:>15,.2f} M")
        gap = self.ltm_nav_change - calc_equity
        if abs(gap) > 0.01:
            print(f"{'Reconciliation Gap:':<45} {gap/1e6:>15,.2f} M")


@dataclass
class PerformanceMetrics:
    """Performance metrics for a fund over a period."""

    ticker: str
    period_type: str
    start_date: str
    end_date: str
    years: float

    # Capital metrics
    capital_calls: float
    distributions: float
    nav_start: float
    nav_end: float

    # Return metrics
    tvpi: float  # Total Value to Paid-In (MOIC)
    dpi: float  # Distributions to Paid-In
    rvpi: float  # Residual Value to Paid-In
    irr: float | None  # Internal Rate of Return (annualized)

    # P&L breakdown
    management_fee: float
    net_interest: float
    realized_gains: float
    unrealized_gains: float
    fx_gains: float
    other_expenses: float
    net_pl: float
    pl_return_pct: float  # P&L return as % of average NAV

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class ReconciliationAnalysis:
    """NAV reconciliation accuracy analysis."""

    ticker: str
    period_type: str
    start_date: str
    end_date: str

    # Period counts
    total_periods: int
    reconciled_periods: int

    # Accuracy metrics (at different tolerance levels)
    periods_within_1m: int
    periods_within_2m: int
    periods_within_5m: int
    accuracy_1m_pct: float
    accuracy_2m_pct: float
    accuracy_5m_pct: float

    # Aggregates
    total_nav_change: float
    total_calculated_change: float
    total_difference: float
    avg_abs_difference: float
    max_abs_difference: float

    # Problem periods (|diff| > 2M)
    problem_periods: list[dict[str, Any]]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
