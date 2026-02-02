#!/usr/bin/env python3
"""Analyze XBRL concepts used by each issuer to create custom mappings.

This script:
1. Scans all XBRL files for an issuer
2. Identifies which concepts are used and their values
3. Groups concepts by category (NAV, income, expense, etc.)
4. Generates issuer-specific mapping configurations
5. Saves to output/{ticker}/xbrl_mapping.json
"""

import json
import re
from pathlib import Path
from xml.etree import ElementTree as ET
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple


# Known namespaces
NAMESPACES = {
    'http://www.bmv.com.mx/2015-06-30/ccd': 'mx_ccd',
    'http://www.cnbv.gob.mx/2015-06-30/ccd': 'mx_ccd',
    'http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full': 'ifrs',
}

# Concept categories based on keywords
CATEGORY_KEYWORDS = {
    'nav': ['Equity', 'NetAssets', 'NetAssetsLiabilities'],
    'issued_capital': ['IssuedCapital', 'ShareCapital', 'ContributedCapital'],
    'management_fee': ['ManagementFee', 'FeesCostsAndExpensesOfTheAdministrator', 'AdministratorFee'],
    'interest_income': ['InterestIncome', 'RevenueFromInterest', 'FinancialIncome'],
    'interest_expense': ['InterestExpense', 'AccruedInterestExpense', 'FinanceCosts', 'FinancialExpenses'],
    'realized_gains': ['RealizedGain', 'SaleOfProperty', 'GainOnDisposal', 'RevenueFromDividends'],
    'unrealized_gains': ['GainOnValuation', 'IncomeFromChangeInFairValue', 'FairValueGain', 'IncomeFromRevaluation'],
    'unrealized_losses': ['LossOnValuation', 'LossOnChangeInFairValue', 'FairValueLoss', 'LossOnChangesInFairValue'],
    'fx_gains': ['ForeignExchangeIncome', 'GainOnForeignExchange', 'ForeignExchangeGain'],
    'fx_losses': ['ForeignExchangeLoss', 'LossOnForeignExchange'],
    'other_expenses': ['ProfessionalFees', 'OtherAdministrativeExpenses', 'Taxes', 'MaintenanceCosts',
                       'AdministrativeExpense', 'OtherExpense', 'Advertising', 'InsurancesAndGuarantees'],
    'capital_calls': ['IssueAndPlacement', 'ProceedsFromIssuing', 'ContributionFromHolders'],
    'distributions': ['DividendsPaid', 'DistributionToHolders', 'Redemptions'],
    'total_assets': ['Assets', 'TotalAssets'],
    'total_liabilities': ['Liabilities', 'TotalLiabilities'],
    'cash': ['CashAndCashEquivalents', 'Cash', 'BalancesWithBanks'],
    'investments': ['FinancialInstruments', 'InvestmentsInSubsidiaries', 'DesignatedFinancialInstruments'],
    'revenue': ['Revenue', 'GrossProfit', 'TotalRevenue'],
    'profit_loss': ['ProfitLoss', 'ProfitLossBeforeTax', 'ComprehensiveIncome'],
}

# Concepts to exclude (duplicates, non-numeric, etc.)
EXCLUDE_PATTERNS = [
    'Explanatory', 'Description', 'Disclosure', 'Policy', 'Member', 'Axis',
    'Dimension', 'Domain', 'Abstract', 'Total', 'Label'
]


def get_namespace_prefix(ns: str) -> str:
    """Get short prefix for namespace."""
    return NAMESPACES.get(ns, 'other')


def categorize_concept(local_name: str) -> str:
    """Categorize a concept based on its name."""
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in local_name.lower():
                return category
    return 'uncategorized'


def should_exclude(local_name: str) -> bool:
    """Check if concept should be excluded."""
    return any(pattern.lower() in local_name.lower() for pattern in EXCLUDE_PATTERNS)


def parse_context_type(context_id: str, contexts: Dict) -> str:
    """Determine if context is instant or duration."""
    if context_id in contexts:
        return contexts[context_id]
    # Heuristic based on context ID patterns
    if 'instant' in context_id.lower() or context_id.startswith('I'):
        return 'instant'
    return 'duration'


def extract_contexts(root: ET.Element) -> Dict[str, str]:
    """Extract context definitions from XBRL."""
    contexts = {}
    for elem in root.iter():
        if 'context' in elem.tag.lower():
            ctx_id = elem.get('id')
            if ctx_id:
                # Check for instant vs period
                has_instant = any('instant' in child.tag.lower() for child in elem.iter())
                has_period = any('period' in child.tag.lower() and 'instant' not in child.tag.lower()
                               for child in elem.iter())
                if has_instant:
                    contexts[ctx_id] = 'instant'
                elif has_period:
                    contexts[ctx_id] = 'duration'
    return contexts


def analyze_xbrl_file(xbrl_path: Path) -> Dict:
    """Analyze a single XBRL file to extract concept usage."""
    try:
        tree = ET.parse(xbrl_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"  Error parsing {xbrl_path.name}: {e}")
        return {}

    contexts = extract_contexts(root)
    concepts = defaultdict(lambda: {'values': [], 'contexts': set(), 'context_types': set()})

    for elem in root.iter():
        tag = elem.tag
        if not tag.startswith('{'):
            continue

        # Parse namespace and local name
        ns_end = tag.find('}')
        if ns_end == -1:
            continue

        ns = tag[1:ns_end]
        local_name = tag[ns_end+1:]

        # Skip excluded concepts
        if should_exclude(local_name):
            continue

        # Get value
        try:
            value = float(elem.text) if elem.text and elem.text.strip() else None
        except (ValueError, TypeError):
            continue

        if value is None:
            continue

        # Get context
        ctx_ref = elem.get('contextRef', '')
        ctx_type = parse_context_type(ctx_ref, contexts)

        # Store
        prefix = get_namespace_prefix(ns)
        key = f"{prefix}:{local_name}"
        concepts[key]['values'].append(value)
        concepts[key]['contexts'].add(ctx_ref)
        concepts[key]['context_types'].add(ctx_type)
        concepts[key]['namespace'] = ns
        concepts[key]['local_name'] = local_name

    return dict(concepts)


def analyze_issuer(ticker: str, xbrl_dir: Path) -> Dict:
    """Analyze all XBRL files for an issuer."""
    print(f"\nAnalyzing {ticker}...")

    all_concepts = defaultdict(lambda: {
        'values': [],
        'files': 0,
        'context_types': set(),
        'namespace': '',
        'local_name': '',
        'category': '',
        'non_zero_count': 0,
        'sample_values': []
    })

    xbrl_files = list(xbrl_dir.glob('*.xbrl'))
    print(f"  Found {len(xbrl_files)} XBRL files")

    for xbrl_path in xbrl_files:
        file_concepts = analyze_xbrl_file(xbrl_path)
        for key, data in file_concepts.items():
            all_concepts[key]['values'].extend(data['values'])
            all_concepts[key]['files'] += 1
            all_concepts[key]['context_types'].update(data.get('context_types', set()))
            all_concepts[key]['namespace'] = data.get('namespace', '')
            all_concepts[key]['local_name'] = data.get('local_name', '')

    # Post-process
    for key, data in all_concepts.items():
        data['category'] = categorize_concept(data['local_name'])
        data['context_types'] = list(data['context_types'])
        non_zero = [v for v in data['values'] if v != 0]
        data['non_zero_count'] = len(non_zero)
        data['total_count'] = len(data['values'])
        # Sample values (last 5 non-zero)
        data['sample_values'] = non_zero[-5:] if non_zero else []
        # Summary stats
        if non_zero:
            data['min'] = min(non_zero)
            data['max'] = max(non_zero)
            data['avg'] = sum(non_zero) / len(non_zero)
        else:
            data['min'] = data['max'] = data['avg'] = 0
        # Remove full values list to save space
        del data['values']

    return dict(all_concepts)


def generate_mapping_config(ticker: str, concepts: Dict) -> Dict:
    """Generate a mapping configuration for an issuer."""

    # Group by category
    by_category = defaultdict(list)
    for key, data in concepts.items():
        if data['non_zero_count'] > 0:  # Only include concepts with data
            by_category[data['category']].append({
                'concept': key,
                'namespace': data['namespace'],
                'local_name': data['local_name'],
                'context_types': data['context_types'],
                'non_zero_count': data['non_zero_count'],
                'files': data['files'],
                'sample_values': data['sample_values'],
                'avg': data['avg'],
            })

    # Sort each category by non_zero_count descending
    for cat in by_category:
        by_category[cat].sort(key=lambda x: x['non_zero_count'], reverse=True)

    # Build mapping config
    config = {
        'ticker': ticker,
        'generated_at': datetime.now().isoformat(),
        'total_concepts_found': len(concepts),
        'concepts_with_data': sum(1 for c in concepts.values() if c['non_zero_count'] > 0),

        # Recommended mappings (best candidate per category)
        'mappings': {},

        # All candidates per category for manual review
        'candidates': dict(by_category),
    }

    # Select best mapping for each category
    for category in CATEGORY_KEYWORDS.keys():
        candidates = by_category.get(category, [])
        if candidates:
            # Prefer instant context for balance sheet items, duration for P&L
            balance_sheet_cats = ['nav', 'issued_capital', 'total_assets', 'total_liabilities', 'cash', 'investments']
            preferred_ctx = 'instant' if category in balance_sheet_cats else 'duration'

            # Sort by: preferred context type, then by count
            def score(c):
                ctx_match = 1 if preferred_ctx in c['context_types'] else 0
                return (ctx_match, c['non_zero_count'])

            candidates.sort(key=score, reverse=True)
            best = candidates[0]

            config['mappings'][category] = {
                'concept': best['concept'],
                'namespace': best['namespace'],
                'local_name': best['local_name'],
                'sign': 1,  # Default, may need manual adjustment
                'context_type': preferred_ctx if preferred_ctx in best['context_types'] else best['context_types'][0] if best['context_types'] else 'duration',
                'confidence': 'high' if best['non_zero_count'] >= 5 else 'medium' if best['non_zero_count'] >= 2 else 'low',
                'alternatives': [c['concept'] for c in candidates[1:4]],  # Top 3 alternatives
            }

    return config


def print_summary(ticker: str, config: Dict):
    """Print a summary of the mapping analysis."""
    print(f"\n{'='*70}")
    print(f"  {ticker} - XBRL CONCEPT MAPPING ANALYSIS")
    print(f"{'='*70}")
    print(f"  Total concepts found: {config['total_concepts_found']}")
    print(f"  Concepts with data: {config['concepts_with_data']}")
    print()

    print(f"  {'Category':<20} {'Recommended Concept':<35} {'Conf':<8} {'Count':>6}")
    print(f"  {'-'*70}")

    for category, mapping in config['mappings'].items():
        concept = mapping['concept']
        if len(concept) > 33:
            concept = concept[:30] + '...'
        conf = mapping['confidence']
        # Get count from candidates
        count = next((c['non_zero_count'] for c in config['candidates'].get(category, [])
                     if c['concept'] == mapping['concept']), 0)
        print(f"  {category:<20} {concept:<35} {conf:<8} {count:>6}")

    # Show uncategorized concepts that might be important
    uncategorized = config['candidates'].get('uncategorized', [])
    if uncategorized:
        print(f"\n  Uncategorized concepts with significant data:")
        for c in uncategorized[:10]:
            if c['non_zero_count'] >= 3:
                print(f"    - {c['concept']}: {c['non_zero_count']} occurrences, avg={c['avg']:,.0f}")


def main():
    output_dir = Path('output')

    # Top 10 CERPIs
    tickers = ['AYLLUPI', 'CAPGLPI', 'GLOB1PI', 'LOCKXPI', 'BLKPEPI',
               'QTZALPI', 'SVPI', 'HV2PI', 'CAPI', 'KANANPI']

    all_configs = {}

    for ticker in tickers:
        xbrl_dir = output_dir / ticker / 'xbrls'
        if not xbrl_dir.exists():
            print(f"Skipping {ticker} - no xbrls directory")
            continue

        # Analyze
        concepts = analyze_issuer(ticker, xbrl_dir)

        # Generate config
        config = generate_mapping_config(ticker, concepts)
        all_configs[ticker] = config

        # Save individual config
        config_path = output_dir / ticker / 'xbrl_mapping.json'
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2, default=str)
        print(f"  Saved mapping to {config_path}")

        # Print summary
        print_summary(ticker, config)

    # Save combined config
    combined_path = output_dir / 'xbrl_mappings_all.json'
    with open(combined_path, 'w') as f:
        json.dump(all_configs, f, indent=2, default=str)
    print(f"\n\nCombined mappings saved to {combined_path}")


if __name__ == '__main__':
    main()
