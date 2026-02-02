"""XBRL Taxonomy concept mappings for Mexican financial instruments."""

# XBRL Taxonomy Concepts
# Balance sheet concepts (instant - point in time)
BALANCE_SHEET_CONCEPTS: dict[str, str] = {
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Equity": "equity",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}IssuedCapital": "issued_capital",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}RetainedEarnings": "retained_earnings",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Assets": "assets",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CashAndCashEquivalents": "cash",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CurrentAssets": "current_assets",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}NoncurrentAssets": "noncurrent_assets",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}InvestmentsInSubsidiariesJointVenturesAndAssociates": "investments",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}Liabilities": "liabilities",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}CurrentLiabilities": "current_liabilities",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}NoncurrentLiabilities": "noncurrent_liabilities",
}

# P&L concepts (duration - period of time)
# Note: We extract both quarterly (90-day) and YTD contexts
PL_CONCEPTS: dict[str, str] = {
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}ProfitLoss": "profit_loss",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}DividendsPaidClassifiedAsFinancingActivities": "dividends_paid",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}IssueAndPlacementOfStockCertificates": "capital_calls",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}NetContributionOfHoldersOfIssuanceAndPlacementCosts": "net_contributions",
}

# Additional P&L concepts for detailed analysis
DETAILED_PL_CONCEPTS: dict[str, str] = {
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}ManagementFee": "management_fee",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}RevenueFromInterest": "interest_income",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}FinanceCosts": "interest_expense",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}RealizedGainOfAssetsDesignatedAtFairValue": "realized_gains",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}AdjustmentsForFairValueGainsLosses": "unrealized_gains",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}LossOnChangesInFairValueOfFinancialInstruments": "unrealized_losses",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}GainOnForeignExchange": "fx_gains",
    "{http://www.cnbv.gob.mx/2015-06-30/ccd}ForeignExchangeLoss": "fx_losses",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}AdministrativeExpense": "admin_expense",
    "{http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full}DividendsPaid": "distributions",
}

# Namespace prefixes for common taxonomies
NAMESPACE_PREFIXES: dict[str, str] = {
    "ifrs": "http://xbrl.ifrs.org/taxonomy/2014-03-05/ifrs-full",
    "mx_ccd": "http://www.cnbv.gob.mx/2015-06-30/ccd",
    "mx_ifrs": "http://www.cnbv.gob.mx/2015-06-30/ifrs",
}


def get_concept_mapping(concept_key: str) -> dict | None:
    """Get full concept mapping info for a given key.

    Args:
        concept_key: The short name (e.g., 'equity', 'profit_loss')

    Returns:
        Dict with concept info or None if not found
    """
    # Search in balance sheet concepts
    for full_name, key in BALANCE_SHEET_CONCEPTS.items():
        if key == concept_key:
            # Parse the full name to extract namespace and local name
            if full_name.startswith("{"):
                ns_end = full_name.index("}")
                namespace = full_name[1:ns_end]
                local_name = full_name[ns_end + 1:]
            else:
                namespace = ""
                local_name = full_name

            return {
                "full_name": full_name,
                "namespace": namespace,
                "local_name": local_name,
                "context_type": "instant",
                "category": "balance_sheet",
            }

    # Search in P&L concepts
    for full_name, key in PL_CONCEPTS.items():
        if key == concept_key:
            if full_name.startswith("{"):
                ns_end = full_name.index("}")
                namespace = full_name[1:ns_end]
                local_name = full_name[ns_end + 1:]
            else:
                namespace = ""
                local_name = full_name

            return {
                "full_name": full_name,
                "namespace": namespace,
                "local_name": local_name,
                "context_type": "duration",
                "category": "pl",
            }

    # Search in detailed P&L concepts
    for full_name, key in DETAILED_PL_CONCEPTS.items():
        if key == concept_key:
            if full_name.startswith("{"):
                ns_end = full_name.index("}")
                namespace = full_name[1:ns_end]
                local_name = full_name[ns_end + 1:]
            else:
                namespace = ""
                local_name = full_name

            return {
                "full_name": full_name,
                "namespace": namespace,
                "local_name": local_name,
                "context_type": "duration",
                "category": "pl_detailed",
            }

    return None


def get_all_concepts() -> dict[str, dict]:
    """Get all concept mappings.

    Returns:
        Dict mapping concept keys to their full info
    """
    result = {}

    for concepts, context_type, category in [
        (BALANCE_SHEET_CONCEPTS, "instant", "balance_sheet"),
        (PL_CONCEPTS, "duration", "pl"),
        (DETAILED_PL_CONCEPTS, "duration", "pl_detailed"),
    ]:
        for full_name, key in concepts.items():
            if full_name.startswith("{"):
                ns_end = full_name.index("}")
                namespace = full_name[1:ns_end]
                local_name = full_name[ns_end + 1:]
            else:
                namespace = ""
                local_name = full_name

            result[key] = {
                "full_name": full_name,
                "namespace": namespace,
                "local_name": local_name,
                "context_type": context_type,
                "category": category,
            }

    return result
