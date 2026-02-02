#!/usr/bin/env python3
"""Extract fund sizes from downloaded CERPI XBRL files."""

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

OUTPUT_DIR = Path("output")

# XBRL namespaces
NS = {
    "xbrli": "http://www.xbrl.org/2003/instance",
    "ifrs-full": "http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full",
}

# Approximate USD/MXN rate for comparison
USD_MXN = 20.5


def parse_xbrl(filepath: Path) -> dict:
    """Parse XBRL file and extract key financial data."""
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except Exception as e:
        return {"error": str(e)}

    # Find currency
    currency = "USD"
    for elem in root.iter():
        if "unit" in elem.tag.lower():
            for child in elem.iter():
                if child.text and "MXN" in child.text:
                    currency = "MXN"
                    break
                elif child.text and "USD" in child.text:
                    currency = "USD"

    # Find all contexts to identify the latest period
    contexts = {}
    for ctx in root.findall(".//xbrli:context", NS):
        ctx_id = ctx.get("id")
        instant = ctx.find(".//xbrli:instant", NS)
        if instant is not None and instant.text:
            contexts[ctx_id] = instant.text

    # Find total assets - get the most recent value
    assets_values = []
    for elem in root.iter():
        if elem.tag.endswith("}Assets") and not "Current" in elem.tag and not "NonCurrent" in elem.tag:
            ctx_ref = elem.get("contextRef")
            period = contexts.get(ctx_ref, "unknown")
            try:
                value = int(elem.text)
                decimals = elem.get("decimals", "0")
                assets_values.append({
                    "value": value,
                    "period": period,
                    "decimals": decimals,
                })
            except (ValueError, TypeError):
                pass

    # Find total equity values
    equity_values = []
    for elem in root.iter():
        if elem.tag.endswith("}Equity"):
            ctx_ref = elem.get("contextRef")
            period = contexts.get(ctx_ref, "unknown")
            try:
                value = int(elem.text)
                if value > 0:
                    decimals = elem.get("decimals", "0")
                    equity_values.append({
                        "value": value,
                        "period": period,
                        "decimals": decimals,
                    })
            except (ValueError, TypeError):
                pass

    # Get the latest values
    latest_assets = None
    if assets_values:
        assets_values.sort(key=lambda x: x["period"], reverse=True)
        latest_assets = assets_values[0]

    latest_equity = None
    if equity_values:
        equity_values.sort(key=lambda x: (x["period"], -x["value"]), reverse=True)
        latest_period = equity_values[0]["period"]
        period_equities = [e for e in equity_values if e["period"] == latest_period]
        latest_equity = max(period_equities, key=lambda x: x["value"])

    return {
        "currency": currency,
        "assets": latest_assets,
        "equity": latest_equity,
    }


def format_val(value: int, currency: str) -> str:
    """Format value with currency."""
    symbol = "$" if currency == "USD" else "MX$"
    if value >= 1_000_000_000:
        return f"{symbol}{value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"{symbol}{value / 1_000_000:.1f}M"
    else:
        return f"{symbol}{value:,.0f}"


def main():
    summary_path = OUTPUT_DIR / "cerpi_xbrl_downloads.json"
    with open(summary_path) as f:
        downloads = json.load(f)

    results = []

    for item in downloads:
        ticker = item["ticker"]
        filepath = item.get("file")

        if not filepath or not Path(filepath).exists():
            results.append({
                "ticker": ticker,
                "id": item["id"],
                "currency": None,
                "assets": None,
                "equity": None,
                "assets_usd": None,
            })
            continue

        data = parse_xbrl(Path(filepath))

        if "error" in data:
            results.append({
                "ticker": ticker,
                "id": item["id"],
                "currency": None,
                "assets": None,
                "equity": None,
                "assets_usd": None,
                "error": data["error"],
            })
            continue

        assets = data.get("assets")
        equity = data.get("equity")
        currency = data.get("currency", "USD")

        assets_val = assets["value"] if assets else None
        equity_val = equity["value"] if equity else None
        period = assets["period"] if assets else (equity["period"] if equity else None)

        # Convert to USD for comparison
        if assets_val:
            assets_usd = assets_val if currency == "USD" else assets_val / USD_MXN
        else:
            assets_usd = None

        results.append({
            "ticker": ticker,
            "id": item["id"],
            "report_period": item.get("period"),
            "balance_date": period,
            "currency": currency,
            "assets": assets_val,
            "equity": equity_val,
            "assets_usd": assets_usd,
        })

        assets_str = format_val(assets_val, currency) if assets_val else "N/A"
        equity_str = format_val(equity_val, currency) if equity_val else "N/A"
        print(f"{ticker:12} | {currency} | Assets: {assets_str:>14} | Equity: {equity_str:>14} | {period or 'N/A'}")

    # Sort by USD value for comparison
    results.sort(key=lambda x: x["assets_usd"] or 0, reverse=True)

    # Save results
    results_path = OUTPUT_DIR / "cerpi_sizes.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)

    # Summary
    print("\n" + "=" * 80)
    print("SORTED BY SIZE (USD equivalent):")
    print("=" * 80)
    print(f"{'Ticker':<12} {'Ccy':>4} {'Assets':>16} {'Assets (USD eq)':>16}")
    print("-" * 80)

    total_usd = 0
    for r in results:
        if r["assets"]:
            ccy = r["currency"]
            assets_str = format_val(r["assets"], ccy)
            usd_eq = r["assets_usd"]
            total_usd += usd_eq or 0
            usd_str = f"${usd_eq/1e9:.2f}B" if usd_eq >= 1e9 else f"${usd_eq/1e6:.1f}M"
            print(f"{r['ticker']:<12} {ccy:>4} {assets_str:>16} {usd_str:>16}")

    print("=" * 80)
    print(f"{'TOTAL':.<12} {'':>4} {'':>16} ${total_usd/1e9:.2f}B USD")

    # CSV
    import csv
    csv_path = OUTPUT_DIR / "cerpi_sizes.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "id", "report_period", "balance_date", "currency", "assets", "equity", "assets_usd"])
        writer.writeheader()
        writer.writerows(results)
    print(f"\nSaved: {results_path}, {csv_path}")


if __name__ == "__main__":
    main()
