# eSIM Pricing Automation Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=flat&logo=pandas&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-latest-F7931E?style=flat&logo=scikit-learn&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google_Sheets_API-v4-34A853?style=flat&logo=google-sheets&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.x-000000?style=flat&logo=flask&logoColor=white)

> End-to-end automated pricing engine that generates competitive sell prices for **10,800+ SKUs** based on real customer usage behaviour — replacing a fully manual process.

---

## Description

A data pipeline built for a telecom reseller operating in the eSIM market. The system ingests raw daily usage and subscription data, computes behaviour-adjusted cost estimates per SKU, and publishes competitive prices directly to Google Sheets — automatically, every run.

| | |
|---|---|
| **Product types** | BIG DATA · FUP (Fixed-speed Unlimited) · Pure Unlimited |
| **Countries covered** | 16+ roaming destinations |
| **SKUs generated** | 10,800+ per run |
| **Pricing model error** | ~5.7% vs. market benchmark |

---

## Features

- **Behaviour Score Engine** — P50/P75 usage ratio blend per package with 5-tier interpolation fallback (EXACT → REGION → GLOBAL → NEAREST → CURVE)
- **Quota Feasibility Factor (QFF)** — physics-informed clamp layer that self-calibrates from real usage data each run
- **Weighted Modal Cost** — dynamic supplier cost derived from MCC-resolved country usage distribution, not static tables
- **Market-calibrated pricing formula** — log-linear model reverse-engineered from competitor data with 5.7% mean error
- **Isotonic regression smoothing** — enforces monotonic score curves per product type via scikit-learn
- **Looker Studio BI export** — 11-sheet business intelligence export (executive summary, country breakdown, SKU profitability, churn/anomaly detection)
- **ICCID Checker web app** — local LAN Flask app for per-SIM usage and cost lookup by team

---

## Tech Stack

| Layer | Tools |
|---|---|
| Language | Python 3.10+ |
| Data processing | Pandas, NumPy |
| Statistical modelling | SciPy (`curve_fit`, `lstsq`), scikit-learn (`IsotonicRegression`) |
| API integration | gspread, Google Sheets API v4, Google Drive API |
| Web app | Flask, psycopg2, PostgreSQL |
| Config | JSON-based country, MCC, and rate mappings |
| Output | Google Sheets (live publish), Excel (.xlsx), CSV (Looker Studio) |

---

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL (for the web app only)
- Google Cloud project with Sheets API + Drive API enabled
- Service account credentials JSON

### Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd pricing-pipeline

# 2. Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add Google credentials
# Place your service account JSON at:
#   config/google_credentials.json

# 5. Configure data mappings (edit to match your product codes)
#   data/mappings/country_map.json   — product code → country/group/region
#   data/mappings/country_rate.json  — country → CNY/GB rate
#   data/mappings/mcc_map.json       — MCC code → country name
```

### Web app (optional, requires PostgreSQL)

```bash
cd webapp
cp .env.example .env
# Fill in: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

pip install -r requirements.txt
python app.py
# Team access: http://<your-local-ip>:5000
```

---

## Usage

### Run full pricing pipeline

```bash
python main.py
```

Reads XLSX files from `data/raw/`, runs all pipeline stages, publishes results to Google Sheets.

### Export Looker Studio / BI data

```bash
python -m exporters.looker_export
```

Outputs 11 CSV files + 1 Excel workbook to `data/output/looker/`.

### Launch ICCID Checker web app

```bash
cd webapp && python app.py
```

Access at `http://localhost:5000`. For LAN team access, use `http://<your-ip>:5000`.

---

## Pipeline Architecture

```
Raw XLSX (daily usage + subscriptions)
    │
    ▼
Loaders — normalise ICCID, parse MCC area codes, convert KB → MB
    │
    ▼
Subscription Processor — match usage to subscription windows, compute usage ratio
    │
    ▼
Behaviour Factor Engine
  ├── P50/P75 blend per package
  ├── 5-tier interpolation: EXACT → REGION → GLOBAL → NEAREST → CURVE
  ├── QFF clamping (self-calibrating each run)
  └── Isotonic regression smoothing
    │
    ▼
Full SKU Matrix — (country × quota × 1–30 days) for all known countries
    │
    ▼
Pricing Calculation
  ├── Weighted modal cost (MCC-resolved country rates)
  ├── Product-type pricing formula (log-linear for BIG DATA)
  └── SIM / eSIM / flat prices in IDR
    │
    ▼
Google Sheets publish + Excel export
```

---

## Project Structure

```
pricing-pipeline/
├── config/
│   ├── mappings.py               # Re-exports from behaviour_factor
│   └── paths.py                  # Project root + data path definitions
├── data/
│   ├── mappings/                 # country_map.json, mcc_map.json, country_rate.json
│   ├── raw/                      # Input XLSX files (gitignored)
│   └── output/                   # Pipeline outputs (gitignored)
├── loaders/
│   ├── load_daily.py             # Daily usage ingestion (parallel)
│   └── load_subscription.py      # Subscription data ingestion
├── processors/
│   ├── behaviour_factor.py       # Core BF + QFF engine + interpolation
│   ├── subscription_processor.py # Usage-to-subscription join + ratio calc
│   ├── cost_calculator.py        # Weighted modal cost
│   └── normalize_cross_type.py   # Monotonic price normalization
├── exporters/
│   ├── google_sheets_exporter.py # Pricing calc + live Sheets publish
│   ├── db_exporter.py            # PostgreSQL export
│   └── looker_export.py          # 11-sheet BI export (CSV + Excel)
├── rules/
│   └── quota_rules.py            # Quota MB extraction from package names
├── pivots/
│   └── country_usage_pivot.py
├── summaries/
│   └── summary.py
├── webapp/
│   ├── app.py                    # Flask ICCID Checker (local LAN)
│   ├── templates/index.html      # Web UI
│   ├── requirements.txt
│   └── .env.example
├── main.py                       # Pipeline entry point
└── README.md
```

---

## Future Improvements

- [ ] Unify CNY→IDR conversion rate across all modules (currently split: 2450 vs 2650)
- [ ] Replace positional `iloc` column access in loaders with named columns
- [ ] Expand `mcc_map.json` from 56 entries to full ITU MCC list (~250 countries)
- [ ] Add CLI flags to `main.py` (e.g. `--dry-run`, `--skip-export`, `--date-range`)
- [ ] Schedule pipeline runs via cron / Windows Task Scheduler
- [ ] Telegram or email alerts on EXPIRING_SOON / ANOMALY_HIGH flags from BI export
- [ ] Unit tests for quota extraction, BF interpolation, and cost calculation

---

## Privacy Note

Repository contains source code only. All customer data, supplier pricing, credentials, and raw XLSX files are excluded via `.gitignore`.
