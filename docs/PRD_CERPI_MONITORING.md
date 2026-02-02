# PRD: CERPI Monitoring System

## 1. Problem Statement

We need to systematically monitor Mexican CERPIs (Certificados de Proyectos de Inversión) to:
- Track competitor capital commitments and deployment patterns
- Understand average ticket sizes by investor type
- Monitor fund performance at the most granular level available (subserie > serie > fund)
- Map underlying funds/investments even without explicit names

**Constraint:** We cannot connect CERPIs to specific siefores (AFOREs pension funds), but we can track aggregate investor behavior through capital movements.

## 2. Current State

### Data Sources
| Source | Data Available | Granularity |
|--------|----------------|-------------|
| BIVA API | Issuer info, emissions, documents | Fund, Serie |
| BMV Scraping | Documents, securities list | Fund, Serie |
| XBRL Quarterly Reports | NAV, P&L, balance sheet, capital flows | Fund (some subserie data in dimensions) |

### Existing SQLite Schema
```sql
-- nav_reconciliation: Quarterly fund-level NAV tracking
-- cash_flows: Capital calls/distributions for IRR calculation
```

### 35+ CERPIs Currently Tracked
Total AUM: ~$43B USD equivalent

## 3. Proposed PostgreSQL Schema

### 3.1 Core Reference Tables (`db_*` prefix)

```sql
-- ============================================================================
-- REFERENCE DATA (slowly changing dimensions)
-- ============================================================================

-- Fund/CERPI master
CREATE TABLE db_cerpi (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,     -- CAPGLPI, LOCKXPI, etc.
    biva_id INTEGER,                         -- BIVA issuer ID
    bmv_id INTEGER,                          -- BMV issuer ID
    name VARCHAR(255),                       -- Full legal name
    fideicomiso VARCHAR(50),                 -- F/187788, etc.
    manager VARCHAR(100),                    -- GP name (e.g., "BlackRock", "Blackstone")
    trustee VARCHAR(100),                    -- Fiduciario (e.g., "Banco Nacional de México")
    currency VARCHAR(3) DEFAULT 'USD',       -- Reporting currency
    inception_date DATE,
    status VARCHAR(20) DEFAULT 'ACTIVE',     -- ACTIVE, CLOSED, LIQUIDATING
    fund_type VARCHAR(50),                   -- PE, VC, Real Estate, Infrastructure, etc.
    vintage_year INTEGER,                    -- Fund vintage
    target_size BIGINT,                      -- Target commitment (local currency)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Series within a CERPI
CREATE TABLE db_serie (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    serie VARCHAR(20) NOT NULL,              -- A, B, C, UNICA, etc.
    isin VARCHAR(20),
    tipo_valor VARCHAR(20),                  -- CKD, CERPI, FIBRA
    emission_date DATE,
    maturity_date DATE,
    initial_placement BIGINT,                -- Initial títulos en circulación
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cerpi_id, serie)
);

-- Subseries (when available)
CREATE TABLE db_subserie (
    id SERIAL PRIMARY KEY,
    serie_id INTEGER REFERENCES db_serie(id),
    subserie VARCHAR(20) NOT NULL,           -- 1, 2, 3 or A1, A2, etc.
    description VARCHAR(255),
    fee_class VARCHAR(20),                   -- Full fee, reduced fee, no fee
    investor_type VARCHAR(50),               -- Institutional, Siefore, GP, etc.
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(serie_id, subserie)
);

-- Underlying funds/investments (anonymized if needed)
CREATE TABLE db_underlying_fund (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    fund_code VARCHAR(20) NOT NULL,          -- "Fund 1", "Fund 2" or actual name if known
    fund_name VARCHAR(255),                  -- Actual name if disclosed
    manager VARCHAR(100),                    -- GP of underlying fund
    strategy VARCHAR(100),                   -- PE Buyout, VC Growth, Real Assets, etc.
    geography VARCHAR(100),                  -- US, Europe, LatAm, Global
    vintage_year INTEGER,
    currency VARCHAR(3),
    first_observed DATE,                     -- When we first saw this fund
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cerpi_id, fund_code)
);

-- Investor tracking (anonymized)
-- Since we can't link to siefores, track at aggregate level
CREATE TABLE db_investor_class (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,       -- "SIEFORE", "Aseguradora", "GP", "Other"
    description VARCHAR(255)
);
```

### 3.2 Time Series Tables (`ts_*` prefix)

```sql
-- ============================================================================
-- TIME SERIES DATA (append-only, immutable facts)
-- ============================================================================

-- Fund-level NAV and capital (quarterly)
CREATE TABLE ts_fund_nav (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    period VARCHAR(10) NOT NULL,             -- 2025Q3
    balance_date DATE NOT NULL,

    -- NAV tracking
    nav BIGINT,                              -- Equity/NAV
    nav_prior BIGINT,                        -- Prior period NAV
    nav_change BIGINT,                       -- NAV delta

    -- Capital structure
    issued_capital BIGINT,                   -- Total issued capital
    retained_earnings BIGINT,                -- Accumulated P&L

    -- P&L components
    management_fee BIGINT DEFAULT 0,
    interest_income BIGINT DEFAULT 0,
    interest_expense BIGINT DEFAULT 0,
    realized_gains BIGINT DEFAULT 0,
    unrealized_gains BIGINT DEFAULT 0,
    unrealized_losses BIGINT DEFAULT 0,
    fx_gains BIGINT DEFAULT 0,
    fx_losses BIGINT DEFAULT 0,
    other_expenses BIGINT DEFAULT 0,

    -- Capital movements (derived from issued_capital delta)
    capital_calls BIGINT DEFAULT 0,
    distributions BIGINT DEFAULT 0,

    -- Reconciliation
    calculated_change BIGINT,
    reconciliation_diff BIGINT,

    -- Metadata
    source_file VARCHAR(255),
    is_definitive BOOLEAN DEFAULT FALSE,     -- Quarterly (T) vs Definitive (D)
    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(cerpi_id, period, is_definitive)
);

-- Serie-level capital tracking (when available from XBRL dimensions)
CREATE TABLE ts_serie_capital (
    id SERIAL PRIMARY KEY,
    serie_id INTEGER REFERENCES db_serie(id),
    period VARCHAR(10) NOT NULL,
    balance_date DATE NOT NULL,

    -- Capital
    issued_capital BIGINT,
    nav BIGINT,

    -- Flows
    capital_calls BIGINT DEFAULT 0,
    distributions BIGINT DEFAULT 0,

    -- Títulos
    titulos_circulacion BIGINT,              -- Outstanding certificates

    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(serie_id, period)
);

-- Subserie-level capital tracking (when available)
CREATE TABLE ts_subserie_capital (
    id SERIAL PRIMARY KEY,
    subserie_id INTEGER REFERENCES db_subserie(id),
    period VARCHAR(10) NOT NULL,
    balance_date DATE NOT NULL,

    issued_capital BIGINT,
    nav BIGINT,
    capital_calls BIGINT DEFAULT 0,
    distributions BIGINT DEFAULT 0,
    titulos_circulacion BIGINT,

    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(subserie_id, period)
);

-- Underlying fund investments (positions)
CREATE TABLE ts_fund_investment (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    underlying_fund_id INTEGER REFERENCES db_underlying_fund(id),
    period VARCHAR(10) NOT NULL,
    balance_date DATE NOT NULL,

    -- Position
    commitment BIGINT,                       -- Total commitment to underlying
    contributed BIGINT,                      -- Capital contributed to date
    distributed BIGINT,                      -- Distributions received to date
    fair_value BIGINT,                       -- Current FMV of investment

    -- Derived metrics
    remaining_commitment BIGINT,             -- commitment - contributed
    total_value BIGINT,                      -- fair_value + distributed
    moic DECIMAL(10,2),                      -- total_value / contributed

    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cerpi_id, underlying_fund_id, period)
);

-- Capital events (individual calls/distributions)
CREATE TABLE ts_capital_event (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    serie_id INTEGER REFERENCES db_serie(id),        -- NULL if fund-level
    subserie_id INTEGER REFERENCES db_subserie(id),  -- NULL if serie-level

    event_date DATE NOT NULL,
    event_type VARCHAR(20) NOT NULL,         -- CAPITAL_CALL, DISTRIBUTION
    amount BIGINT NOT NULL,                  -- Positive for calls, positive for distributions

    -- Call details (when known)
    call_number INTEGER,                     -- 1st, 2nd, 3rd call
    purpose VARCHAR(100),                    -- Investment, fees, expenses

    -- Distribution details (when known)
    distribution_type VARCHAR(50),           -- Return of capital, gain, dividend

    -- Source
    source_document VARCHAR(255),            -- Evento relevante ID

    created_at TIMESTAMP DEFAULT NOW()
);

-- Balance sheet positions (for detailed analysis)
CREATE TABLE ts_balance_sheet (
    id SERIAL PRIMARY KEY,
    cerpi_id INTEGER REFERENCES db_cerpi(id),
    period VARCHAR(10) NOT NULL,
    balance_date DATE NOT NULL,

    -- Assets
    cash_and_equivalents BIGINT,
    investments_private_funds BIGINT,
    investments_subsidiaries BIGINT,
    investment_property BIGINT,
    other_current_assets BIGINT,
    other_noncurrent_assets BIGINT,
    total_assets BIGINT,

    -- Liabilities
    current_liabilities BIGINT,
    noncurrent_liabilities BIGINT,
    total_liabilities BIGINT,

    -- Equity
    equity_nav BIGINT,

    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cerpi_id, period)
);
```

### 3.3 Analytical Views

```sql
-- ============================================================================
-- ANALYTICAL VIEWS
-- ============================================================================

-- Latest fund metrics
CREATE VIEW v_fund_latest AS
SELECT
    c.ticker,
    c.manager,
    c.fund_type,
    c.vintage_year,
    n.period,
    n.balance_date,
    n.nav,
    n.issued_capital,
    n.capital_calls,
    n.distributions,
    -- Performance metrics
    COALESCE(SUM(n2.capital_calls), 0) as total_calls_itd,
    COALESCE(SUM(n2.distributions), 0) as total_distributions_itd,
    -- DPI = distributions / contributions
    CASE WHEN SUM(n2.capital_calls) > 0
         THEN ROUND(SUM(n2.distributions)::numeric / SUM(n2.capital_calls), 2)
         ELSE 0 END as dpi,
    -- RVPI = NAV / contributions
    CASE WHEN SUM(n2.capital_calls) > 0
         THEN ROUND(n.nav::numeric / SUM(n2.capital_calls), 2)
         ELSE 0 END as rvpi,
    -- TVPI = (NAV + distributions) / contributions
    CASE WHEN SUM(n2.capital_calls) > 0
         THEN ROUND((n.nav + SUM(n2.distributions))::numeric / SUM(n2.capital_calls), 2)
         ELSE 0 END as tvpi
FROM db_cerpi c
JOIN ts_fund_nav n ON c.id = n.cerpi_id
JOIN ts_fund_nav n2 ON c.id = n2.cerpi_id
    AND n2.balance_date <= n.balance_date
WHERE n.balance_date = (
    SELECT MAX(balance_date) FROM ts_fund_nav WHERE cerpi_id = c.id
)
GROUP BY c.ticker, c.manager, c.fund_type, c.vintage_year,
         n.period, n.balance_date, n.nav, n.issued_capital,
         n.capital_calls, n.distributions;

-- Competitor deployment pace
CREATE VIEW v_deployment_pace AS
SELECT
    c.ticker,
    c.manager,
    n.period,
    n.capital_calls,
    n.nav,
    -- Rolling 4-quarter deployment
    SUM(n2.capital_calls) OVER (
        PARTITION BY c.id
        ORDER BY n.balance_date
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as ltm_calls,
    -- Average quarterly call size
    AVG(n2.capital_calls) OVER (
        PARTITION BY c.id
        ORDER BY n.balance_date
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) as avg_quarterly_call
FROM db_cerpi c
JOIN ts_fund_nav n ON c.id = n.cerpi_id
JOIN ts_fund_nav n2 ON c.id = n2.cerpi_id
WHERE n.capital_calls > 0
ORDER BY c.manager, n.balance_date;

-- Average ticket size by manager (competitor analysis)
CREATE VIEW v_manager_ticket_sizes AS
SELECT
    c.manager,
    COUNT(DISTINCT c.id) as num_funds,
    SUM(n.issued_capital) as total_aum,
    AVG(n.issued_capital) as avg_fund_size,
    -- Estimate average ticket from capital calls
    AVG(CASE WHEN e.amount > 0 THEN e.amount END) as avg_call_size,
    COUNT(DISTINCT e.id) FILTER (WHERE e.event_type = 'CAPITAL_CALL') as total_calls,
    COUNT(DISTINCT e.id) FILTER (WHERE e.event_type = 'DISTRIBUTION') as total_distributions
FROM db_cerpi c
JOIN ts_fund_nav n ON c.id = n.cerpi_id
LEFT JOIN ts_capital_event e ON c.id = e.cerpi_id
WHERE n.balance_date = (
    SELECT MAX(balance_date) FROM ts_fund_nav WHERE cerpi_id = c.id
)
GROUP BY c.manager
ORDER BY total_aum DESC;
```

## 4. Data Mapping

### 4.1 XBRL to Database Mapping

| XBRL Concept | Database Column | Table |
|--------------|-----------------|-------|
| `ifrs:Equity` | `nav` | ts_fund_nav |
| `ifrs:IssuedCapital` | `issued_capital` | ts_fund_nav |
| `mx_ccd:ManagementFee` | `management_fee` | ts_fund_nav |
| `mx_ccd:InterestIncome` | `interest_income` | ts_fund_nav |
| `ifrs:DividendsPaidClassifiedAsFinancingActivities` | `distributions` | ts_fund_nav |
| `ifrs:Assets` | `total_assets` | ts_balance_sheet |
| `mx_ccd:InvestmentsInPrivateFunds` | `investments_private_funds` | ts_balance_sheet |

### 4.2 Series/Subserie Detection

XBRL files may contain dimensional data for series breakdown. Look for:
- `xbrldi:explicitMember dimension="mx_ccd:SeriesAxis"` contexts
- `xbrldi:explicitMember dimension="mx_ccd:ComponentsOfEquityAxis"` contexts

When present, extract values per dimension member to populate `ts_serie_capital`.

### 4.3 Underlying Fund Mapping

Since fund names aren't always disclosed:
1. Track unique `InvestmentsInPrivateFunds` line items per CERPI
2. Assign sequential codes: "Fund 1", "Fund 2", etc.
3. Update `db_underlying_fund.fund_name` when disclosed in prospectus/events
4. Use fair value changes to track individual fund performance

## 5. Key Metrics for Competitor Analysis

### 5.1 Deployment Metrics
- **Capital call frequency**: How often does competitor call capital?
- **Call size distribution**: Average/median call size
- **Deployment pace**: LTM calls / total commitment
- **Investment period utilization**: How far into commitment period?

### 5.2 Performance Metrics (at fund level)
- **TVPI** (Total Value to Paid-In): (NAV + Distributions) / Contributions
- **DPI** (Distributions to Paid-In): Distributions / Contributions
- **RVPI** (Residual Value to Paid-In): NAV / Contributions
- **IRR**: Using ts_capital_event for actual cash flows

### 5.3 Fee Analysis
- Management fee as % of NAV
- Management fee as % of committed capital
- Fee drag on returns

## 6. Implementation Phases

### Phase 1: Core Schema & Migration
- [ ] Create PostgreSQL schema
- [ ] Migrate existing SQLite data
- [ ] Load historical XBRL data

### Phase 2: Serie/Subserie Extraction
- [ ] Detect dimensional XBRL contexts
- [ ] Parse serie-level data when available
- [ ] Build subserie tracking

### Phase 3: Underlying Fund Mapping
- [ ] Extract investment positions from balance sheets
- [ ] Create anonymous fund mappings
- [ ] Track position changes over time

### Phase 4: Event Capture
- [ ] Parse "Eventos Relevantes" for capital calls/distributions
- [ ] Extract specific call/distribution amounts
- [ ] Link to prospectus data for commitments

### Phase 5: Analytics & Dashboards
- [ ] Implement analytical views
- [ ] Build competitor comparison queries
- [ ] Create monitoring alerts

## 7. Data Quality Considerations

### Reconciliation Tolerance
- Accept NAV reconciliation within 5% or $5M (whichever is smaller)
- Flag larger discrepancies for manual review

### Missing Data Handling
- Use NULL for truly unknown values
- Use 0 for confirmed zero values
- Document data source gaps

### Deduplication
- Keep both preliminary (T) and definitive (D) filings
- Use `is_definitive` flag for filtering
- Prefer definitive when analyzing performance

## 8. Open Questions

1. **Siefore Attribution**: Can we infer investor types from capital call patterns/sizes without explicit disclosure?

2. **Cross-Fund Holdings**: Some CERPIs may invest in other CERPIs - should we track these relationships?

3. **FX Treatment**: Store in original currency or convert to USD for comparison?

4. **Real-time vs Batch**: XBRL files are quarterly - do we need more frequent event monitoring?

---

## Appendix: Sample Queries

### A. Top CERPIs by deployment pace
```sql
SELECT ticker, manager, ltm_calls, avg_quarterly_call
FROM v_deployment_pace
WHERE period = (SELECT MAX(period) FROM ts_fund_nav)
ORDER BY ltm_calls DESC
LIMIT 10;
```

### B. Managers with most active capital calling
```sql
SELECT manager, num_funds, total_calls, avg_call_size
FROM v_manager_ticket_sizes
ORDER BY total_calls DESC;
```

### C. Underperforming funds (TVPI < 1.0)
```sql
SELECT ticker, manager, nav, total_calls_itd, tvpi
FROM v_fund_latest
WHERE tvpi < 1.0
ORDER BY tvpi ASC;
```
