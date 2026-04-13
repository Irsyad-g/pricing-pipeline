# eSIM Pricing Automation Pipeline

> End-to-end automated pricing engine that generates competitive sell prices for 10,800+ SKUs based on real customer usage behaviour — replacing a fully manual process.

---

## Overview

A data pipeline built for a telecom reseller operating in the eSIM market. The system ingests raw daily usage and subscription data, computes behaviour-adjusted cost estimates per SKU, and publishes competitive prices directly to Google Sheets.

**Product types covered:** BIG DATA · FUP (Fixed-speed) · Pure Unlimited  
**Coverage:** 16+ countries · 30-day duration range · Multiple quota tiers  
**Output:** 10,800+ SKUs priced and published automatically per run

---

## Pipeline Architecture

```
Raw XLSX (daily + subscription)
    │
    ▼
Loaders (load_daily, load_subscription)
    │
    ▼
Subscription Processor
  - Match ICCID usage to subscription windows
  - Calculate usage ratio per package
  - Extract country-level usage distribution
    │
    ▼
Behaviour Factor Engine
  - Compute P50/P75 usage ratio per package
  - 5-tier interpolation: EXACT → REGION → GLOBAL → NEAREST → CURVE
  - Quota Feasibility Factor (QFF) clamp
  - Isotonic regression smoothing
    │
    ▼
Full SKU Matrix Generator
  - Generate all (country × quota × duration) combinations
  - Include new-market countries via force_include flag
    │
    ▼
Pricing Exporter
  - Weighted modal cost from MCC-based country usage
  - Market-calibrated pricing formula (log-linear, reverse-engineered)
  - Price smoothing per product type
  - Publish to Google Sheets
```

---

## Key Components

### 1. Behaviour Score Engine

Computes a usage ratio (weighted P50/P75 blend) per package from ICCID-level daily data. Uses a **5-tier interpolation chain** to fill coverage gaps:

| Tier | Condition | Min Samples |
|------|-----------|-------------|
| EXACT | Country + duration + quota match | 15 |
| REGION | Same region, same duration + quota | — |
| GLOBAL | All countries, same duration + quota | — |
| NEAREST | Closest duration (±5 days), same quota | — |
| CURVE | Mathematical fallback formula | — |

Multi-region packages (e.g. ASIA covering ASIA_EAST + ASIA_SEA) propagate scores into both regional lookup tables, improving coverage for individual country SKUs.

### 2. Quota Feasibility Factor (QFF)

A physics-informed clamp layer applied before smoothing. Prevents unrealistic behaviour scores by anchoring them to product-type logic:

- **BIG DATA:** `qff = (daily_reference / gb_per_day) * 0.8` — calibrated from actual usage data each run
- **FUP:** Duration-decay curve based on measured activity rates. Higher quota tiers have lower base QFF (less likely to fully consume high-speed allocation)
- **Unlimited:** Decay from 0.75 at day 1 toward 0.35 floor — engagement drops on longer subscriptions

Scores are blended with QFF using **sample-weighted confidence**:

```
EXACT (500+ samples) → 92% data weight, 8% QFF
EXACT (15 samples)   → 65% data weight, 35% QFF
REGION               → 60% data weight, 40% QFF
CURVE                → 10% data weight, 90% QFF
```

### 3. Weighted Modal Cost

Replaces static supplier cost tables with a **dynamic per-package modal** based on real roaming patterns:

```
1. Extract country usage per ICCID from daily data
2. Resolve MCC codes → country names (via mcc_map.json)
3. Lookup CNY/GB rate per country (via country_rate.json)
4. Weighted average: Σ(usage_weight × country_rate)
```

Falls back to a static `MODAL_REFERENCE` sheet for new packages with no usage history.

### 4. Market-Calibrated Pricing Formula

BIG DATA pricing was re-engineered by reverse-engineering competitor prices from a comparative dataset. A log-linear model was fit with **5.7% mean error**:

```
rate = 0.5848 + 0.0650 × ln(day) − 0.1016 × ln(GB)
MODAL_ADJ = modal × GB × behaviour_score × rate
```

This replaced an aggressive linear day-factor that caused 50–200% overpricing on long-duration packages. Result: average gap vs. market reduced from **+53% → target 10–15%**.

FUP and Unlimited retain behaviour-score-based pricing with duration decay.

---

## Technical Highlights

- **Isotonic regression smoothing** — BIG DATA scores are monotonically non-decreasing by duration (longer access = more accumulated usage). FUP/Unlimited scores are monotonically decreasing (engagement drops over time)
- **Price smoothing by product type** — BIG DATA prices are floor-enforced. FUP/Unlimited allow decay but cap single-step increases at 30%
- **Self-calibrating QFF** — daily reference and activity rate constants recompute each run from actual data, replacing hardcoded assumptions
- **Force-include for new markets** — countries flagged in `country_map.json` are included in the SKU matrix even with zero historical data
- **Granular region mapping** — ASIA split into ASIA_EAST and ASIA_SEA for more accurate regional interpolation

---

## Stack

| Layer | Tools |
|-------|-------|
| Language | Python 3.10+ |
| Data processing | Pandas, NumPy |
| Statistical modelling | SciPy (`curve_fit`, `lstsq`), scikit-learn (`IsotonicRegression`) |
| API integration | gspread, Google Sheets API v4, Google Drive API |
| Config | JSON-based country/MCC/rate mappings |
| Output | Google Sheets (live publish) + Excel (.xlsx) |

---

## Project Structure

```
project/
├── config/
│   ├── mappings.py          # Global config constants
│   └── paths.py             # File path definitions
├── data/
│   ├── mappings/            # country_map.json, mcc_map.json, group mappings
│   └── raw/                 # Input XLSX files (gitignored)
├── loaders/
│   ├── load_daily.py        # Daily usage ingestion
│   └── load_subscription.py # Subscription data ingestion
├── processors/
│   ├── behaviour_factor.py  # Core behaviour score + QFF engine
│   ├── subscription_processor.py
│   └── country_distribution.py
├── exporters/
│   ├── google_sheets_exporter.py  # Pricing calc + publish
│   └── excel_exporter.py
├── rules/
│   └── quota_rules.py       # Quota extraction logic
├── summaries/               # Monthly and aggregate summaries
├── pivots/                  # Country usage pivot tables
└── main.py                  # Pipeline entry point
```

---

## Privacy Note

This repository contains only source code. All customer data, supplier pricing, and credentials are excluded via `.gitignore`.
