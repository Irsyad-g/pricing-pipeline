# eSIM Pricing Automation Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=flat&logo=pandas&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-latest-F7931E?style=flat&logo=scikit-learn&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=flat&logo=postgresql&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google_Sheets_API-v4-34A853?style=flat&logo=google-sheets&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.x-000000?style=flat&logo=flask&logoColor=white)

> A production pricing and margin intelligence system built for a multi-country telecom eSIM reseller. It covers the full chain from cost analysis and sell-price generation to revenue reconciliation across all sales channels.

---

## Overview

A data pipeline serving an eSIM operator with roaming products across Africa, Asia, Europe, the Americas, and Oceania. The system ingests raw ICCID-level usage and subscription data, computes behaviour-adjusted cost estimates per SKU, publishes competitive sell prices to Google Sheets, and reconciles multi-channel marketplace revenue against actual COGS. The entire process runs without manual steps.

| Metric | Value |
|--------|-------|
| SKUs generated per run | 10,800+ |
| Roaming countries covered | 16+ |
| Pricing model error vs. market benchmark | ~5.7% |
| Product types | BIG DATA · FUP · Pure Unlimited |
| Sales channels reconciled | Shopee · Tokopedia · Shopify |

---

## System Architecture

```
Raw XLSX (daily usage + subscriptions + marketplace orders)
    │
    ├─► Loaders: normalise ICCID, parse MCC area codes, convert KB → MB
    │
    ├─► Subscription Processor: match usage to subscription windows, compute ratio
    │
    ├─► Behaviour Factor Engine
    │     ├── P50/P75 usage blend per SKU package
    │     ├── 5-tier interpolation: EXACT → REGION → GLOBAL → NEAREST → CURVE
    │     ├── Quota Feasibility Factor (QFF): self-calibrating physics clamp
    │     └── Isotonic regression smoothing (scikit-learn)
    │
    ├─► Pricing Calculation
    │     ├── Weighted modal cost (MCC-resolved country rates)
    │     ├── Log-linear formula calibrated from competitor data
    │     └── SIM / eSIM / flat prices in IDR
    │
    ├─► Margin & Revenue Reconciliation
    │     ├── Multi-channel order ingestion (Shopee, Tokopedia, Shopify)
    │     ├── SKU normalisation with cross-platform fallback mapping
    │     ├── ICCID-level cost matching via PostgreSQL
    │     ├── Per-unit price correction (Shopify order-total → per-ICCID)
    │     └── RUGI / NORMAL / BAGUS margin classification
    │
    └─► Outputs
          ├── Google Sheets (live pricing + margin dashboard)
          ├── Excel workbook (formula-driven, auditable)
          ├── 11-sheet Looker Studio BI export
          └── LAN Flask web app (ICCID lookup for ops team)
```

---

## Core Methodology

### 1. Behaviour Score Engine

The central innovation of the pricing model. Instead of pricing on quota alone, the engine computes a **behaviour score** (0–1) representing how intensively a customer segment actually uses a given package.

- **P50/P75 usage ratio blend**: a weighted median/75th-percentile mix that avoids outlier sensitivity
- **5-tier interpolation fallback** when direct sample data is insufficient:
  1. `EXACT`: direct sample from the same SKU
  2. `REGION`: same geographic region (e.g. SEA, EU)
  3. `GLOBAL`: all-product average
  4. `NEAREST`: closest quota/duration match
  5. `CURVE`: parametric curve fit from aggregate data
- **Isotonic regression smoothing** (`sklearn.isotonic.IsotonicRegression`) enforces monotonic score curves per product type, so longer durations never produce paradoxically higher scores than shorter ones

### 2. Quota Feasibility Factor (QFF)

A physics-informed clamping layer that prevents the pricing model from generating unrealistic prices for high-quota packages in low-throughput markets.

- Self-calibrates each run from real usage data, with no hardcoded thresholds
- Derived from GB/day throughput distributions per country × product type
- Applied as a multiplicative clamp before the pricing formula

### 3. Weighted Modal Cost

Cost per SKU is calculated as a weighted average across the actual roaming countries visited by customers on that package, not a static table lookup.

- MCC (Mobile Country Code) strings extracted from raw usage records
- Mapped to country names and CNY/GB rates via `mcc_map.json` and `country_rate.json`
- Usage MB weights applied per country, then aggregated to a single CNY cost per subscription

### 4. Log-Linear Pricing Formula

The final sell price is computed via a log-linear model reverse-engineered from competitor pricing across 3 product types (BIG DATA, FUP, Unlimited), with quota and duration as predictors.

- Mean absolute error: **~5.7%** vs. market benchmark on held-out SKUs
- Monotonic price smoothing is enforced post-formula via a 3-pass algorithm (cummax → minimum growth rate floor → spike cap), which prevents pricing inversions and plateaus at IDR rounding boundaries

### 5. Multi-Channel Margin Reconciliation

The margin engine ingests order exports from Shopee, Tokopedia, and Shopify, normalises them to a canonical SKU format, and matches each ICCID against the PostgreSQL cost database.

- **SKU normalisation**: regex prefix rules combined with a JSON fallback map handle rebranding, marketplace-specific prefixes (`WG-`, `GM-`, `SM-`), and historical SKU migrations
- **Shopify per-unit correction**: Shopify exports order totals rather than per-unit prices, so the engine divides by the pre-deduplication item count per invoice to get the correct per-ICCID figure
- **Backlog tracking**: unmatched ICCIDs are written to `processed.margin_backlog` in PostgreSQL and auto-resolved on subsequent runs when the subscription data comes through
- **Margin classification**: each ICCID is tagged RUGI (loss) / NORMAL / BAGUS (≥30% margin), with conditional formatting in the Excel output and country-level breakdowns

---

## Features

| Feature | Description |
|---------|-------------|
| Behaviour Score Engine | P50/P75 blend with 5-tier interpolation and isotonic smoothing |
| Quota Feasibility Factor | Self-calibrating physics clamp derived from real usage data |
| Weighted Modal Cost | MCC-resolved dynamic COGS, not a static rate lookup |
| Log-linear pricing formula | Market-calibrated model with ~5.7% error vs. benchmark |
| Multi-channel reconciliation | Shopee, Tokopedia, Shopify with a unified margin view |
| Margin classification | RUGI / NORMAL / BAGUS per ICCID with backlog tracking |
| Looker Studio BI export | 11-sheet export covering executive summary, country analysis, SKU profitability, and churn/anomaly detection |
| ICCID Checker web app | Local LAN Flask app for per-SIM usage, cost, and country breakdown |
| Live Google Sheets publish | Pricing and margin data pushed automatically each run |
| Formula-driven Excel output | All calculations in Excel formulas, fully auditable by non-technical stakeholders |

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Language | Python 3.10+ |
| Data processing | Pandas, NumPy |
| Statistical modelling | SciPy (`curve_fit`, `lstsq`), scikit-learn (`IsotonicRegression`) |
| Database | PostgreSQL (SQLAlchemy, psycopg2) |
| API integration | gspread, Google Sheets API v4, Google Drive API |
| Web app | Flask |
| Output formats | Google Sheets (live), Excel (openpyxl), CSV (Looker Studio) |
| Config | JSON-based country, MCC, SKU, and rate mappings |

---

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL (for the web app and margin engine)
- Google Cloud project with Sheets API + Drive API enabled
- Service account credentials JSON

### Setup

```bash
# 1. Clone
git clone https://github.com/Irsyad-g/pricing-pipeline.git
cd pricing-pipeline

# 2. Virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux

# 3. Dependencies
pip install -r requirements.txt

# 4. Google credentials
# Place service account JSON at: config/google_credentials.json

# 5. Configure mappings
# data/mappings/country_map.json  - product code to country / group / region
# data/mappings/country_rate.json - country to CNY/GB rate  (gitignored, add manually)
# data/mappings/mcc_map.json      - MCC code to country name
```

### Web app (requires PostgreSQL)

```bash
cd webapp
cp .env.example .env
# Fill in: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
python app.py
# Team access: http://<your-local-ip>:5000
```

---

## Usage

### Full pricing pipeline

```bash
python main.py
```

Reads XLSX files from `data/raw/`, runs all pipeline stages, and publishes pricing to Google Sheets.

### Looker Studio / BI export

```bash
python -m exporters.looker_export
```

Outputs 11 CSV files and 1 Excel workbook to `data/output/looker/`.

### ICCID Checker web app

```bash
cd webapp && python app.py
```

---

## Project Structure

```
pricing-pipeline/
├── config/
│   ├── commission.py             # Channel x product commission rates
│   └── paths.py                  # Project root and data path definitions
├── data/
│   ├── mappings/                 # country_map.json, mcc_map.json, sku_fallback.json
│   ├── raw/                      # Input XLSX files (gitignored)
│   └── output/                   # Pipeline outputs (gitignored)
├── loaders/
│   ├── load_daily.py             # Daily usage ingestion
│   └── load_subscription.py      # Subscription data ingestion
├── processors/
│   ├── behaviour_factor.py       # Core BF + QFF engine + 5-tier interpolation
│   ├── subscription_processor.py # Usage-to-subscription join and ratio calculation
│   ├── cost_calculator.py        # Weighted modal cost + order matching
│   └── normalize_cross_type.py   # Monotonic price normalisation
├── exporters/
│   ├── google_sheets_exporter.py # Pricing calc + live Sheets publish
│   ├── db_exporter.py            # PostgreSQL export
│   └── looker_export.py          # 11-sheet BI export (CSV + Excel)
├── rules/
│   └── quota_rules.py            # Quota MB extraction from package names
├── pivots/
│   └── country_usage_pivot.py    # Country usage cross-tab
├── summaries/
│   └── summary.py                # Aggregate usage summaries
├── webapp/
│   ├── app.py                    # Flask ICCID Checker (local LAN)
│   ├── templates/index.html      # Web UI
│   └── .env.example
├── main.py                       # Pricing pipeline entry point
└── README.md
```

---

## Privacy

Repository contains source code only. All customer data, supplier pricing, credentials, raw XLSX files, and operational scripts are excluded via `.gitignore`.
