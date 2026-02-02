#!/usr/bin/env python3
"""Extract balance sheet data from QTZALPI quarterly XBRL files."""

import re
import csv
from pathlib import Path
from xml.etree import ElementTree as ET

# XBRL namespaces
NS = {
    'xbrli': 'http://www.xbrl.org/2003/instance',
    'ifrs-full': 'http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full',
    'mx_ccd': 'http://www.bmv.com.mx/2015-06-30/ccd',
}

# Concepts to extract (tag name -> friendly name)
CONCEPTS = {
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashAndCashEquivalents': 'cash_and_equivalents',
    '{http://www.bmv.com.mx/2015-06-30/ccd}InvestmentsInPrivateFunds': 'investments_private_funds',
    '{http://www.cnbv.gob.mx/2015-06-30/ccd}InvestmentsInPrivateFunds': 'investments_private_funds',  # CNBV namespace
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}InvestmentsInSubsidiariesJointVenturesAndAssociates': 'investments_subsidiaries',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}InvestmentProperty': 'investment_property',
    '{http://www.bmv.com.mx/2015-06-30/ccd}FinancialInstruments': 'financial_instruments',
    '{http://www.cnbv.gob.mx/2015-06-30/ccd}FinancialInstruments': 'financial_instruments',  # CNBV namespace (QTZALPI)
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Assets': 'total_assets',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Liabilities': 'total_liabilities',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Equity': 'equity_nav',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CurrentAssets': 'current_assets',
    '{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}NoncurrentAssets': 'noncurrent_assets',
}


def parse_quarterly_filename(filename: str) -> dict | None:
    """Extract date, quarter, and year from quarterly report filename.

    Handles multiple QTZALPI naming patterns:
    - ReporteTrimestral_QTZALPI-CIB3079_2024_2_80876-241119-25dba2fb.xbrl
    - ReporteTrimestral_4DT_2022_QTZALPI_CIB3079_1688166809143.xbrl
    - bivaFR2_19_15774.xbrl (with date prefix like 20201125_)
    - Información trimestral 1-2020 pattern
    """
    # Pattern 1: New format - ReporteTrimestral_TICKER_YEAR_QUARTER_...
    match = re.search(r'ReporteTrimestral_QTZALPI-CIB3079_(\d{4})_(\d+D?)_', filename)
    if match:
        year, quarter_str = match.groups()
        quarter = int(quarter_str.replace('D', ''))
        is_definitive = 'D' in quarter_str
        # Extract file date from prefix if present
        date_match = re.match(r'(\d{8})_', filename)
        file_date = date_match.group(1) if date_match else ''
        return {
            'file_date': file_date,
            'quarter': quarter,
            'year': int(year),
            'period': f"{year}Q{quarter}{'D' if is_definitive else ''}",
        }

    # Pattern 2: Old format - ReporteTrimestral_4DT_YEAR_TICKER...
    match = re.search(r'ReporteTrimestral_(\d)D?T_(\d{4})_QTZALPI', filename)
    if match:
        quarter, year = match.groups()
        date_match = re.match(r'(\d{8})_', filename)
        file_date = date_match.group(1) if date_match else ''
        is_definitive = 'DT' in filename or '4DT' in filename
        return {
            'file_date': file_date,
            'quarter': int(quarter),
            'year': int(year),
            'period': f"{year}Q{quarter}{'D' if is_definitive else ''}",
        }

    # Pattern 3: Información trimestral format
    match = re.search(r'Información trimestral (\d)D?-(\d{4})', filename)
    if match:
        quarter, year = match.groups()
        date_match = re.match(r'(\d{8})_', filename)
        file_date = date_match.group(1) if date_match else ''
        is_definitive = 'D-' in filename
        return {
            'file_date': file_date,
            'quarter': int(quarter),
            'year': int(year),
            'period': f"{year}Q{quarter}{'D' if is_definitive else ''}",
        }

    # Pattern 4: Información financiera format (older)
    match = re.search(r'Información financiera (\d)D? - (\d{4})', filename)
    if match:
        quarter, year = match.groups()
        date_match = re.match(r'(\d{8})_', filename)
        file_date = date_match.group(1) if date_match else ''
        is_definitive = 'D ' in filename
        return {
            'file_date': file_date,
            'quarter': int(quarter),
            'year': int(year),
            'period': f"{year}Q{quarter}{'D' if is_definitive else ''}",
        }

    return None


def get_instant_context_date(root: ET.Element) -> str | None:
    """Find the main instant context date (balance sheet date)."""
    for ctx in root.findall('.//xbrli:context', NS):
        period = ctx.find('xbrli:period', NS)
        if period is not None:
            instant = period.find('xbrli:instant', NS)
            if instant is not None:
                # Skip contexts with scenario (dimensional breakdowns)
                if ctx.find('xbrli:scenario', NS) is None:
                    return instant.text
    return None


def extract_balance_sheet(xbrl_path: Path) -> dict | None:
    """Extract balance sheet values from an XBRL file."""
    try:
        tree = ET.parse(xbrl_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing {xbrl_path.name}: {e}")
        return None

    # Get the balance sheet date from instant context
    balance_date = get_instant_context_date(root)

    # Find all contexts without scenario (main values, not dimensional)
    instant_contexts = set()
    for ctx in root.findall('.//xbrli:context', NS):
        if ctx.find('xbrli:scenario', NS) is None:
            period = ctx.find('xbrli:period', NS)
            if period is not None and period.find('xbrli:instant', NS) is not None:
                instant_contexts.add(ctx.get('id'))

    # Extract values
    data = {'balance_date': balance_date}
    for tag, name in CONCEPTS.items():
        # Find the element
        for elem in root.iter():
            if elem.tag == tag:
                ctx_ref = elem.get('contextRef')
                # Only use instant contexts without dimensions
                if ctx_ref in instant_contexts:
                    try:
                        value = int(elem.text) if elem.text else 0
                        data[name] = value
                        break
                    except (ValueError, TypeError):
                        pass
        if name not in data:
            data[name] = None

    return data


def main():
    xbrl_dir = Path('output/QTZALPI/xbrls')

    # Find all quarterly report files (multiple patterns)
    quarterly_files = []
    for f in xbrl_dir.glob('*.xbrl'):
        info = parse_quarterly_filename(f.name)
        if info:
            info['path'] = f
            quarterly_files.append(info)

    print(f"Found {len(quarterly_files)} quarterly report files")

    # Sort by year and quarter
    quarterly_files.sort(key=lambda x: (x['year'], x['quarter'], x.get('file_date', '')))

    # Extract balance sheet data
    results = []
    for qf in quarterly_files:
        data = extract_balance_sheet(qf['path'])
        if data and data.get('total_assets'):  # Only include if we got actual data
            row = {
                'period': qf['period'],
                'year': qf['year'],
                'quarter': qf['quarter'],
                'file_date': qf['file_date'],
                **data
            }
            results.append(row)
            print(f"Processed: {qf['period']} ({qf['path'].name})")

    # Remove duplicates (keep latest file_date for each period)
    seen = {}
    for r in results:
        key = (r['year'], r['quarter'])
        if key not in seen or r['file_date'] > seen[key]['file_date']:
            seen[key] = r

    results = sorted(seen.values(), key=lambda x: (x['year'], x['quarter']))

    # Write to CSV
    output_path = Path('output/QTZALPI/balance_sheet.csv')
    fieldnames = [
        'period', 'year', 'quarter', 'balance_date',
        'cash_and_equivalents', 'financial_instruments', 'investments_private_funds',
        'investments_subsidiaries', 'investment_property', 'current_assets', 'noncurrent_assets',
        'total_assets', 'total_liabilities', 'equity_nav'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWritten {len(results)} quarters to {output_path}")

    # Print summary table
    print("\n" + "="*130)
    print(f"{'Period':<10} {'Balance Date':<12} {'Cash':>15} {'Fin.Instruments':>18} {'Total Assets':>15} {'Liabilities':>15} {'Equity/NAV':>15}")
    print("="*130)
    for r in results:
        cash = f"{r.get('cash_and_equivalents', 0):,.0f}" if r.get('cash_and_equivalents') else 'N/A'
        fin_inst = r.get('financial_instruments', 0) or 0
        fin_str = f"{fin_inst:,.0f}" if fin_inst else 'N/A'
        assets = f"{r.get('total_assets', 0):,.0f}" if r.get('total_assets') else 'N/A'
        liab = f"{r.get('total_liabilities', 0):,.0f}" if r.get('total_liabilities') else 'N/A'
        nav = f"{r.get('equity_nav', 0):,.0f}" if r.get('equity_nav') else 'N/A'
        print(f"{r['period']:<10} {r.get('balance_date', 'N/A'):<12} {cash:>15} {fin_str:>18} {assets:>15} {liab:>15} {nav:>15}")


if __name__ == '__main__':
    main()
