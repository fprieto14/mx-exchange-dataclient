#!/usr/bin/env python3
"""Extract balance sheet + capital calls/distributions from XBRL files."""

import re
import csv
from pathlib import Path
from xml.etree import ElementTree as ET

# Concepts to extract - covers both BMV and CNBV namespaces
CONCEPTS = {
    # Balance sheet
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashAndCashEquivalents': 'cash',
    '{http://www.bmv.com.mx/2015-06-30/ccd}InvestmentsInPrivateFunds': 'investments',
    '{http://www.cnbv.gob.mx/2015-06-30/ccd}InvestmentsInPrivateFunds': 'investments',
    '{http://www.bmv.com.mx/2015-06-30/ccd}FinancialInstruments': 'investments',
    '{http://www.cnbv.gob.mx/2015-06-30/ccd}FinancialInstruments': 'investments',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Assets': 'total_assets',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Liabilities': 'total_liabilities',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Equity': 'equity_nav',

    # Capital structure
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}IssuedCapital': 'issued_capital',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}RetainedEarnings': 'retained_earnings',

    # Capital calls (period flows)
    '{http://www.bmv.com.mx/2015-06-30/ccd}IssueAndPlacementOfStockCertificates': 'capital_calls',
    '{http://www.cnbv.gob.mx/2015-06-30/ccd}IssueAndPlacementOfStockCertificates': 'capital_calls',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}IssueOfEquity': 'issue_of_equity',

    # Distributions (period flows)
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}DividendsPaid': 'distributions',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}DividendsPaidClassifiedAsFinancingActivities': 'distributions',

    # Cash flows
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashFlowsFromUsedInFinancingActivities': 'cf_financing',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashFlowsFromUsedInInvestingActivities': 'cf_investing',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashFlowsFromUsedInOperatingActivities': 'cf_operating',

    # Dividends received from portfolio
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}DividendsReceivedClassifiedAsInvestingActivities': 'dividends_received',
}


def parse_quarterly_filename(filename: str, ticker: str) -> dict | None:
    """Extract quarter info from filename - handles multiple patterns."""
    patterns = [
        # CAPGLPI pattern: ReporteTrimestral_4T_2018_CAPGLPI...
        rf'(\d{{8}})_ReporteTrimestral_(\d)D?T_(\d{{4}})_{ticker}',
        # QTZALPI pattern: ReporteTrimestral_TICKER-TRUST_YEAR_QUARTER...
        rf'(\d{{8}})_.*ReporteTrimestral_{ticker}[^_]*_(\d{{4}})_(\d+D?)_',
        # Información trimestral pattern
        rf'(\d{{8}})_Información trimestral (\d)D?-(\d{{4}})',
        rf'(\d{{8}})_Información financiera (\d)D? - (\d{{4}})',
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                file_date, q, year = groups
                # Handle swapped year/quarter for some patterns
                if len(q) == 4 and q.isdigit():  # year came second
                    year, q = q, year
                quarter = int(str(q).replace('D', ''))
                is_def = 'D' in str(q) or 'DT' in filename
                return {
                    'file_date': file_date,
                    'quarter': quarter,
                    'year': int(year),
                    'period': f"{year}Q{quarter}{'D' if is_def else ''}",
                }
    return None


def extract_values(xbrl_path: Path) -> dict | None:
    """Extract values from an XBRL file."""
    try:
        tree = ET.parse(xbrl_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing {xbrl_path.name}: {e}")
        return None

    # Find instant contexts (for balance sheet items)
    instant_contexts = set()
    period_contexts = set()
    balance_date = None

    for ctx in root.findall('.//{http://www.xbrl.org/2003/instance}context'):
        ctx_id = ctx.get('id')
        # Skip dimensional contexts
        if ctx.find('.//{http://www.xbrl.org/2003/instance}scenario') is not None:
            continue

        period = ctx.find('.//{http://www.xbrl.org/2003/instance}period')
        if period is not None:
            instant = period.find('.//{http://www.xbrl.org/2003/instance}instant')
            if instant is not None:
                instant_contexts.add(ctx_id)
                if balance_date is None:
                    balance_date = instant.text
            else:
                period_contexts.add(ctx_id)

    # Extract values
    data = {'balance_date': balance_date}
    for elem in root.iter():
        if elem.tag in CONCEPTS:
            name = CONCEPTS[elem.tag]
            ctx_ref = elem.get('contextRef')

            # Use instant context for balance sheet, period for flows
            is_flow = name in ['capital_calls', 'distributions', 'issue_of_equity',
                               'cf_financing', 'cf_investing', 'cf_operating', 'dividends_received']

            if is_flow:
                if ctx_ref not in period_contexts:
                    continue
            else:
                if ctx_ref not in instant_contexts:
                    continue

            try:
                value = int(elem.text) if elem.text else 0
                # Keep first non-zero value or first value
                if name not in data or (value != 0 and data.get(name) == 0):
                    data[name] = value
            except (ValueError, TypeError):
                pass

    return data


def process_issuer(ticker: str, xbrl_dir: Path, output_path: Path):
    """Process all quarterly files for an issuer."""
    quarterly_files = []

    for f in xbrl_dir.glob('*.xbrl'):
        info = parse_quarterly_filename(f.name, ticker)
        if info:
            info['path'] = f
            quarterly_files.append(info)

    if not quarterly_files:
        print(f"No quarterly files found for {ticker}")
        return

    quarterly_files.sort(key=lambda x: (x['year'], x['quarter'], x.get('file_date', '')))
    print(f"Found {len(quarterly_files)} quarterly files for {ticker}")

    results = []
    for qf in quarterly_files:
        data = extract_values(qf['path'])
        if data and data.get('total_assets'):
            row = {
                'ticker': ticker,
                'period': qf['period'],
                'year': qf['year'],
                'quarter': qf['quarter'],
                'file_date': qf['file_date'],
                **data
            }
            results.append(row)
            print(f"  {qf['period']}: Assets={data.get('total_assets', 0):,.0f}, "
                  f"CapCalls={data.get('capital_calls', 0):,.0f}, "
                  f"Dist={data.get('distributions', 0):,.0f}")

    # Deduplicate
    seen = {}
    for r in results:
        key = (r['year'], r['quarter'])
        if key not in seen or r['file_date'] > seen[key]['file_date']:
            seen[key] = r

    results = sorted(seen.values(), key=lambda x: (x['year'], x['quarter']))

    # Write CSV
    fieldnames = [
        'ticker', 'period', 'year', 'quarter', 'balance_date',
        'cash', 'investments', 'total_assets', 'total_liabilities', 'equity_nav',
        'issued_capital', 'retained_earnings',
        'capital_calls', 'issue_of_equity', 'distributions',
        'cf_financing', 'cf_investing', 'cf_operating', 'dividends_received'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWritten {len(results)} quarters to {output_path}")
    return results


def main():
    # Process both issuers
    issuers = [
        ('CAPGLPI', Path('output/CAPGLPI/xbrls')),
        ('QTZALPI', Path('output/QTZALPI/xbrls')),
    ]

    all_results = []
    for ticker, xbrl_dir in issuers:
        if xbrl_dir.exists():
            output_path = xbrl_dir.parent / 'fund_flows.csv'
            results = process_issuer(ticker, xbrl_dir, output_path)
            if results:
                all_results.extend(results)

    # Combined summary
    if all_results:
        combined_path = Path('output/combined_fund_flows.csv')
        fieldnames = [
            'ticker', 'period', 'year', 'quarter', 'balance_date',
            'cash', 'investments', 'total_assets', 'total_liabilities', 'equity_nav',
            'issued_capital', 'retained_earnings',
            'capital_calls', 'issue_of_equity', 'distributions',
            'cf_financing', 'cf_investing', 'cf_operating', 'dividends_received'
        ]
        with open(combined_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(sorted(all_results, key=lambda x: (x['ticker'], x['year'], x['quarter'])))
        print(f"\nCombined data written to {combined_path}")


if __name__ == '__main__':
    main()
