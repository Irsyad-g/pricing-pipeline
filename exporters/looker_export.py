"""
Looker Studio Export  — 11 sheets
==================================
Tables:
  processed.final_output   — iccid, package, start_date, end_date,
                             total_quota_mb, total_usage_mb,
                             real_cost_cny, real_cost_idr
  processed.country_usage  — iccid, country ("52010 Indonesia"), country_usage_mb
  processed.margin_backlog — channel, invoice, iccid, sku, product_name,
                             product_type, order_date, harga_jual, reason, resolved

Output → data/output/looker/
  looker_YYYYMMDD_HHMM.xlsx   (all 11 sheets, internal)
  looker_sheetN_name_YYYYMMDD_HHMM.csv  (per sheet, Looker Studio)
"""

import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

_HERE             = Path(__file__).resolve().parent.parent
OUTPUT_DIR        = _HERE / "data/output/looker"
MCC_MAP_PATH      = _HERE / "data/mappings/mcc_map.json"
COUNTRY_RATE_PATH = _HERE / "data/mappings/country_rate.json"

with open(MCC_MAP_PATH) as f:
    MCC_MAP = json.load(f)
with open(COUNTRY_RATE_PATH) as f:
    COUNTRY_RATE = json.load(f)

TODAY = pd.Timestamp.now().normalize()

try:
    from config.commission import get_commission
    _HAS_COMMISSION = True
except Exception:
    get_commission  = None
    _HAS_COMMISSION = False


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _resolve_country(area_str):
    """Parse "52010 Indonesia" → (country_name, mcc_prefix)."""
    s     = str(area_str).strip()
    parts = s.split(" ", 1)
    if len(parts) == 2 and parts[0].isdigit():
        return parts[1].strip(), parts[0]
    m = re.match(r"(\d{3})", s)
    if m:
        return MCC_MAP.get(m.group(1), s), m.group(1)
    return s, ""


def _mb_to_gb(mb, dec=2):
    return round(float(mb) / 1024, dec)


def _extract_days(package, fallback=None):
    """Extract trip duration from package name. e.g. '7 Days' → 7."""
    m = re.search(r"(\d+)\s*days?", str(package), re.IGNORECASE)
    if m:
        return int(m.group(1))
    if fallback is not None and not pd.isna(fallback):
        return int(fallback)
    return None


def _duration_segment(days):
    if days is None or (isinstance(days, float) and np.isnan(days)):
        return "Unknown"
    d = int(days)
    if d <= 3:  return "Short Trip"
    if d <= 7:  return "Week Trip"
    if d <= 14: return "Extended"
    return "Long Stay"

_DURATION_ORDER = ["Short Trip", "Week Trip", "Extended", "Long Stay", "Unknown"]


def _quota_tier(total_quota_mb):
    gb = float(total_quota_mb) / 1024
    if gb <= 3:  return "Lite"
    if gb <= 10: return "Standard"
    if gb <= 20: return "Plus"
    return "Unlimited"

_QUOTA_ORDER = ["Lite", "Standard", "Plus", "Unlimited"]


def _usage_status(pct):
    if pct < 30: return "LOW"
    if pct < 70: return "MEDIUM"
    return "HIGH"


def _top_n_str(country_df_iccid: pd.DataFrame, n: int = 2) -> str:
    """Build 'Indonesia: 824MB (98.5%) | Singapore: 12MB (1.4%)' string."""
    total = country_df_iccid["country_usage_mb"].sum()
    top   = country_df_iccid.nlargest(n, "country_usage_mb")
    parts = []
    for _, r in top.iterrows():
        pct = r["country_usage_mb"] / total * 100 if total else 0
        parts.append(f"{r['country_name']}: {round(r['country_usage_mb'], 1)}MB ({round(pct, 1)}%)")
    return " | ".join(parts)


def _build_top_country_map(country: pd.DataFrame, n: int = 2) -> pd.Series:
    """Return Series iccid → top-N countries string."""
    return (
        country.groupby("iccid")
        .apply(lambda g: _top_n_str(g, n))
        .rename("top_countries")
    )


def _country_cost_dist(final: pd.DataFrame, country: pd.DataFrame) -> pd.DataFrame:
    """
    Proportional cost per (iccid, country):
      cost_idr = (country_usage_mb / iccid_total_usage_mb) * real_cost_idr
    Returns country df with columns: iccid, country_name, mcc_prefix,
      country_usage_mb, country_cost_idr
    """
    iccid_totals = (
        final.groupby("iccid")
        .agg(iccid_total_mb=("total_usage_mb", "sum"),
             iccid_cost_idr=("real_cost_idr",  "sum"))
        .reset_index()
    )
    cu = country.merge(iccid_totals, on="iccid", how="left")
    cu["iccid_total_mb"] = cu["iccid_total_mb"].fillna(0)
    cu["iccid_cost_idr"] = cu["iccid_cost_idr"].fillna(0)
    cu["country_cost_idr"] = np.where(
        cu["iccid_total_mb"] > 0,
        cu["country_usage_mb"] / cu["iccid_total_mb"] * cu["iccid_cost_idr"],
        0,
    )
    return cu


def _enrich_final(final: pd.DataFrame) -> pd.DataFrame:
    """Add days, duration_segment, quota_tier, usage_pct to final_output df."""
    df = final.copy()
    fallback_days = (df["end_date"] - df["start_date"]).dt.days
    df["days"]             = [_extract_days(p, f) for p, f in zip(df["package"], fallback_days)]
    df["duration_segment"] = df["days"].apply(_duration_segment)
    df["quota_tier"]       = df["total_quota_mb"].apply(_quota_tier)
    df["usage_pct"]        = (
        df["total_usage_mb"] / df["total_quota_mb"].replace(0, np.nan) * 100
    ).fillna(0).clip(upper=100).round(1)
    return df


# ════════════════════════════════════════════════════════════════════════════
# LOADERS
# ════════════════════════════════════════════════════════════════════════════

def _load_final(engine) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT iccid, package, start_date, end_date,
               COALESCE(total_quota_mb, 0) AS total_quota_mb,
               COALESCE(total_usage_mb, 0) AS total_usage_mb,
               COALESCE(real_cost_cny, 0)  AS real_cost_cny,
               COALESCE(real_cost_idr, 0)  AS real_cost_idr
        FROM processed.final_output
    """, engine)
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"]   = pd.to_datetime(df["end_date"])
    df["month"]      = df["start_date"].dt.strftime("%Y-%b")
    return df


def _load_country(engine) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT iccid, country,
               SUM(country_usage_mb) AS country_usage_mb
        FROM processed.country_usage
        GROUP BY iccid, country
    """, engine)
    df["country_usage_mb"] = df["country_usage_mb"].astype(float)
    df[["country_name", "mcc_prefix"]] = df["country"].apply(
        lambda v: pd.Series(_resolve_country(v))
    )
    return df


def _load_backlog(engine) -> pd.DataFrame:
    try:
        df = pd.read_sql("""
            SELECT channel, invoice, iccid, sku, product_name,
                   product_type, order_date, harga_jual, reason, resolved
            FROM processed.margin_backlog
        """, engine)
        df["harga_jual"] = pd.to_numeric(df["harga_jual"], errors="coerce")
        return df
    except Exception as e:
        print(f"  [WARN] margin_backlog unavailable: {e}")
        return pd.DataFrame()


# ════════════════════════════════════════════════════════════════════════════
# SHEET 1 — Executive Summary
# ════════════════════════════════════════════════════════════════════════════

def build_executive_summary(ef: pd.DataFrame) -> pd.DataFrame:
    """
    Single overall row with KPIs.
    ef = _enrich_final(final)
    """
    active  = ef[ef["end_date"] >= TODAY]
    expired = ef[ef["end_date"] <  TODAY]

    total_iccid          = ef["iccid"].nunique()
    active_iccid         = active["iccid"].nunique()
    expired_iccid        = expired["iccid"].nunique()
    total_usage_mb       = ef["total_usage_mb"].sum()
    total_usage_gb       = _mb_to_gb(total_usage_mb)
    total_quota_mb       = ef["total_quota_mb"].sum()
    total_cost_idr       = ef["real_cost_idr"].sum()
    avg_usage_pct        = (
        ef["total_usage_mb"].sum() / ef["total_quota_mb"].replace(0, np.nan).sum() * 100
        if total_quota_mb > 0 else 0
    )
    underutilized        = (ef["usage_pct"] < 20).sum()
    heavy_users          = (ef["usage_pct"] > 80).sum()
    expiring_soon        = active[
        (active["end_date"] - TODAY).dt.days <= 3
    ]["iccid"].nunique()

    row = {
        "period_start":            ef["start_date"].min().strftime("%Y-%m-%d"),
        "period_end":              ef["end_date"].max().strftime("%Y-%m-%d"),
        "total_iccid":             total_iccid,
        "total_active_iccid":      active_iccid,
        "total_expired_iccid":     expired_iccid,
        "total_usage_gb":          total_usage_gb,
        "total_quota_gb":          _mb_to_gb(total_quota_mb),
        "avg_usage_per_iccid_gb":  round(total_usage_gb / total_iccid, 2) if total_iccid else 0,
        "total_cost_idr":          round(total_cost_idr, 0),
        "avg_cost_per_iccid":      round(total_cost_idr / total_iccid, 0) if total_iccid else 0,
        "avg_usage_pct":           round(avg_usage_pct, 1),
        "underutilized_count":     int(underutilized),
        "heavy_user_count":        int(heavy_users),
        "expiring_soon_count":     int(expiring_soon),
    }
    return pd.DataFrame([row])


# ════════════════════════════════════════════════════════════════════════════
# SHEET 2 & 7 — By Country (same data, referenced twice)
# ════════════════════════════════════════════════════════════════════════════

def build_by_country(final: pd.DataFrame, country: pd.DataFrame) -> pd.DataFrame:
    """
    Proportional cost per country across all ICCIDs.
    Used for both Sheet 2 (By Country) and Sheet 7 (Country Performance).
    """
    cu = _country_cost_dist(final, country)

    agg = (
        cu.groupby(["country_name", "mcc_prefix"])
        .agg(
            total_iccid_count  =("iccid",             "nunique"),
            total_usage_mb     =("country_usage_mb",  "sum"),
            total_cost_idr     =("country_cost_idr",  "sum"),
        )
        .reset_index()
    )

    grand_mb = agg["total_usage_mb"].sum()
    agg["total_usage_gb"]        = agg["total_usage_mb"].apply(_mb_to_gb)
    agg["usage_share_pct"]       = (
        agg["total_usage_mb"] / grand_mb * 100 if grand_mb else 0
    ).round(1)
    agg["cost_per_gb"]           = np.where(
        agg["total_usage_gb"] > 0,
        (agg["total_cost_idr"] / agg["total_usage_gb"]).round(0),
        0,
    )
    agg["avg_usage_gb_per_iccid"] = np.where(
        agg["total_iccid_count"] > 0,
        (agg["total_usage_gb"] / agg["total_iccid_count"]).round(4),
        0,
    )
    agg["total_cost_idr"] = agg["total_cost_idr"].round(0)
    agg["total_usage_mb"] = agg["total_usage_mb"].round(2)

    return (
        agg[[
            "country_name", "mcc_prefix", "total_iccid_count",
            "total_usage_mb", "total_usage_gb", "usage_share_pct",
            "total_cost_idr", "cost_per_gb", "avg_usage_gb_per_iccid",
        ]]
        .sort_values("total_usage_gb", ascending=False)
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 3 — By Duration Segment
# ════════════════════════════════════════════════════════════════════════════

def build_by_duration(ef: pd.DataFrame, country: pd.DataFrame) -> pd.DataFrame:
    cu = _country_cost_dist(ef, country)
    country_top = (
        cu.groupby(["iccid"])
        .apply(lambda g: g.nlargest(1, "country_usage_mb")["country_name"].iloc[0]
               if not g.empty else "")
        .reset_index(name="top_country_raw")
    )

    merged = ef.merge(country_top, on="iccid", how="left")
    merged["top_country_raw"] = merged["top_country_raw"].fillna("")

    agg = (
        merged.groupby("duration_segment")
        .agg(
            iccid_count       =("iccid",       "nunique"),
            avg_usage_pct     =("usage_pct",   "mean"),
            avg_usage_gb_raw  =("total_usage_mb", "mean"),
            avg_cost_idr      =("real_cost_idr",  "mean"),
            total_cost_idr    =("real_cost_idr",  "sum"),
        )
        .reset_index()
    )

    underutil = (
        ef[ef["usage_pct"] < 30]
        .groupby("duration_segment")["iccid"]
        .nunique()
        .reset_index(name="underutilized_count")
    )
    heavy = (
        ef[ef["usage_pct"] > 80]
        .groupby("duration_segment")["iccid"]
        .nunique()
        .reset_index(name="heavy_user_count")
    )

    # Top country per segment (modal)
    top_country = (
        merged.groupby("duration_segment")["top_country_raw"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "")
        .reset_index(name="top_country")
    )

    agg = (
        agg
        .merge(underutil,  on="duration_segment", how="left")
        .merge(heavy,      on="duration_segment", how="left")
        .merge(top_country,on="duration_segment", how="left")
    )
    agg["underutilized_count"] = agg["underutilized_count"].fillna(0).astype(int)
    agg["heavy_user_count"]    = agg["heavy_user_count"].fillna(0).astype(int)
    agg["avg_usage_pct"]       = agg["avg_usage_pct"].round(1)
    agg["avg_usage_gb"]        = agg["avg_usage_gb_raw"].apply(lambda v: _mb_to_gb(v))
    agg["avg_cost_idr"]        = agg["avg_cost_idr"].round(0)
    agg["total_cost_idr"]      = agg["total_cost_idr"].round(0)

    agg["_order"] = agg["duration_segment"].map(
        {s: i for i, s in enumerate(_DURATION_ORDER)}
    ).fillna(99)

    return (
        agg[[
            "duration_segment", "iccid_count", "avg_usage_pct",
            "avg_usage_gb", "avg_cost_idr", "total_cost_idr",
            "underutilized_count", "heavy_user_count", "top_country",
        ]]
        .sort_values("iccid_count", ascending=False)
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 4 — By Quota Tier
# ════════════════════════════════════════════════════════════════════════════

def build_by_quota_tier(ef: pd.DataFrame) -> pd.DataFrame:
    agg = (
        ef.groupby("quota_tier")
        .agg(
            iccid_count   =("iccid",         "nunique"),
            avg_usage_pct =("usage_pct",     "mean"),
            avg_cost_idr  =("real_cost_idr", "mean"),
            total_quota_mb=("total_quota_mb","sum"),
            total_cost_idr=("real_cost_idr", "sum"),
        )
        .reset_index()
    )

    underutil = (
        ef[ef["usage_pct"] < 30]
        .groupby("quota_tier")["iccid"]
        .nunique()
        .reset_index(name="underutilized_count")
    )
    heavy = (
        ef[ef["usage_pct"] > 80]
        .groupby("quota_tier")["iccid"]
        .nunique()
        .reset_index(name="heavy_user_count")
    )

    agg = agg.merge(underutil, on="quota_tier", how="left")
    agg = agg.merge(heavy,     on="quota_tier", how="left")

    agg["underutilized_count"] = agg["underutilized_count"].fillna(0).astype(int)
    agg["heavy_user_count"]    = agg["heavy_user_count"].fillna(0).astype(int)
    agg["avg_usage_pct"]       = agg["avg_usage_pct"].round(1)
    agg["avg_cost_idr"]        = agg["avg_cost_idr"].round(0)
    agg["total_quota_gb"]      = agg["total_quota_mb"].apply(_mb_to_gb)
    agg["cost_efficiency"]     = np.where(
        agg["total_quota_gb"] > 0,
        (agg["total_cost_idr"] / agg["total_quota_gb"]).round(0),
        0,
    )

    agg["_order"] = agg["quota_tier"].map(
        {t: i for i, t in enumerate(_QUOTA_ORDER)}
    ).fillna(99)

    return (
        agg[[
            "quota_tier", "iccid_count", "avg_usage_pct",
            "underutilized_count", "heavy_user_count",
            "avg_cost_idr", "cost_efficiency",
        ]]
        .sort_values("_order")
        .drop(columns="_order")
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 5 — Duration × Quota Matrix (flat, Looker-compatible)
# ════════════════════════════════════════════════════════════════════════════

def build_duration_quota_matrix(ef: pd.DataFrame) -> pd.DataFrame:
    """
    Flat cross-tab: one row per (duration_segment, quota_tier).
    Includes row totals per duration, column totals per quota, grand total.
    """
    base = (
        ef.groupby(["duration_segment", "quota_tier"])
        .agg(
            iccid_count  =("iccid",         "nunique"),
            avg_usage_pct=("usage_pct",     "mean"),
            avg_cost_idr =("real_cost_idr", "mean"),
        )
        .reset_index()
    )
    base["avg_usage_pct"] = base["avg_usage_pct"].round(1)
    base["avg_cost_idr"]  = base["avg_cost_idr"].round(0)

    def _agg_group(df, dur, quot):
        n   = df["iccid"].nunique()
        pct = df["usage_pct"].mean()
        cst = df["real_cost_idr"].mean()
        return {
            "duration_segment": dur,
            "quota_tier":       quot,
            "iccid_count":      n,
            "avg_usage_pct":    round(pct, 1),
            "avg_cost_idr":     round(cst, 0),
        }

    rows = list(base.to_dict("records"))

    # Row totals (per duration)
    for dur, grp in ef.groupby("duration_segment"):
        rows.append(_agg_group(grp, dur, "TOTAL"))

    # Column totals (per quota)
    for quot, grp in ef.groupby("quota_tier"):
        rows.append(_agg_group(grp, "TOTAL", quot))

    # Grand total
    rows.append(_agg_group(ef, "TOTAL", "TOTAL"))

    df_out = pd.DataFrame(rows)

    dur_order  = {s: i for i, s in enumerate(_DURATION_ORDER + ["TOTAL"])}
    quot_order = {t: i for i, t in enumerate(_QUOTA_ORDER + ["TOTAL"])}
    df_out["_d"] = df_out["duration_segment"].map(dur_order).fillna(99)
    df_out["_q"] = df_out["quota_tier"].map(quot_order).fillna(99)

    return (
        df_out
        .sort_values(["_d", "_q"])
        .drop(columns=["_d", "_q"])
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 6 — Top & Bottom Performers
# ════════════════════════════════════════════════════════════════════════════

def build_top_bottom(ef: pd.DataFrame, country: pd.DataFrame) -> pd.DataFrame:
    """Top 20 by usage_gb (HEAVY) + Bottom 20 by usage_pct excluding expired (UNDERUTILIZED)."""
    top_map = _build_top_country_map(country, n=2)

    iccid_agg = (
        ef.groupby("iccid")
        .agg(
            package          =("package",        "first"),
            duration_segment =("duration_segment","first"),
            quota_tier       =("quota_tier",      "first"),
            total_usage_mb   =("total_usage_mb",  "sum"),
            total_quota_mb   =("total_quota_mb",  "sum"),
            real_cost_idr    =("real_cost_idr",   "sum"),
            usage_pct        =("usage_pct",       "mean"),
            end_date         =("end_date",        "max"),
        )
        .reset_index()
    )
    iccid_agg["usage_gb"] = iccid_agg["total_usage_mb"].apply(_mb_to_gb)
    iccid_agg = iccid_agg.merge(top_map, on="iccid", how="left")
    iccid_agg["top_countries"] = iccid_agg["top_countries"].fillna("")
    iccid_agg["usage_pct"]     = iccid_agg["usage_pct"].round(1)
    iccid_agg["real_cost_idr"] = iccid_agg["real_cost_idr"].round(0)

    top20    = iccid_agg.nlargest(20, "usage_gb").copy()
    top20["flag"] = "HEAVY"

    active = iccid_agg[iccid_agg["end_date"] >= TODAY]
    bot20  = active.nsmallest(20, "usage_pct").copy()
    bot20["flag"] = "UNDERUTILIZED"

    cols = [
        "iccid", "package", "duration_segment", "quota_tier",
        "usage_gb", "usage_pct", "real_cost_idr", "top_countries", "flag",
    ]
    return (
        pd.concat([top20[cols], bot20[cols]], ignore_index=True)
        .sort_values(["flag", "usage_gb"], ascending=[True, False])
        .reset_index(drop=True)
        .rename(columns={"real_cost_idr": "cost_idr"})
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 7 — Country Performance (same as Sheet 2)
# ════════════════════════════════════════════════════════════════════════════
# Alias — call build_by_country(). Kept separate for clarity in Excel.


# ════════════════════════════════════════════════════════════════════════════
# SHEET 8 — SKU Profitability
# ════════════════════════════════════════════════════════════════════════════

def build_sku_profitability(
    ef: pd.DataFrame,
    backlog: pd.DataFrame,
    country: pd.DataFrame,
) -> pd.DataFrame:
    """
    Group by package as SKU proxy. Join margin_backlog where available.
    Margin = harga_jual - real_cost_idr.
    """
    # Most-used country per package
    cu_pkg = country.merge(
        ef[["iccid", "package"]].drop_duplicates(), on="iccid", how="left"
    )
    most_country = (
        cu_pkg.groupby("package")["country_name"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "")
        .reset_index(name="most_used_country")
    )

    sku_agg = (
        ef.groupby("package")
        .agg(
            iccid_count   =("iccid",         "nunique"),
            avg_usage_pct =("usage_pct",     "mean"),
            avg_cost_idr  =("real_cost_idr", "mean"),
            total_cost_idr=("real_cost_idr", "sum"),
        )
        .reset_index()
    )
    sku_agg = sku_agg.merge(most_country, on="package", how="left")

    if not backlog.empty and "harga_jual" in backlog.columns:
        bl_agg = (
            backlog.dropna(subset=["harga_jual"])
            .merge(ef[["iccid", "package"]].drop_duplicates(), on="iccid", how="left")
            .groupby("package")
            .agg(avg_harga_jual=("harga_jual", "mean"))
            .reset_index()
        )
        sku_agg = sku_agg.merge(bl_agg, on="package", how="left")
        sku_agg["avg_margin_idr"]  = (sku_agg["avg_harga_jual"] - sku_agg["avg_cost_idr"]).round(0)
        sku_agg["avg_margin_pct"]  = (
            sku_agg["avg_margin_idr"] / sku_agg["avg_harga_jual"].replace(0, np.nan) * 100
        ).round(1)
    else:
        print("  [TODO] margin_backlog empty — margin cols set to NULL (Sheet 8)")
        sku_agg["avg_harga_jual"] = np.nan
        sku_agg["avg_margin_idr"] = np.nan
        sku_agg["avg_margin_pct"] = np.nan

    def _classify(row):
        if pd.isna(row["avg_margin_pct"]): return np.nan, np.nan, np.nan
        p = row["avg_margin_pct"]
        n = row["iccid_count"]
        return (
            int(n * (p < 0)),
            int(n * (0 <= p < 30)),
            int(n * (p >= 30)),
        )

    sku_agg[["rugi_count", "normal_count", "bagus_count"]] = sku_agg.apply(
        lambda r: pd.Series(_classify(r)), axis=1
    )

    sku_agg["avg_usage_pct"] = sku_agg["avg_usage_pct"].round(1)
    sku_agg["avg_cost_idr"]  = sku_agg["avg_cost_idr"].round(0)

    cols = [
        "package", "iccid_count", "avg_usage_pct",
        "avg_cost_idr", "avg_harga_jual",
        "avg_margin_idr", "avg_margin_pct",
        "rugi_count", "normal_count", "bagus_count",
        "most_used_country",
    ]
    sort_col = "avg_margin_pct" if sku_agg["avg_margin_pct"].notna().any() else "avg_cost_idr"
    return (
        sku_agg[cols]
        .rename(columns={"package": "sku"})
        .sort_values(sort_col, ascending=True, na_position="last")
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 9 — Channel Performance
# ════════════════════════════════════════════════════════════════════════════

def build_channel_performance(
    ef: pd.DataFrame,
    backlog: pd.DataFrame,
) -> pd.DataFrame | None:
    """
    Requires margin_backlog with channel + harga_jual.
    Returns None if data unavailable.
    """
    if backlog.empty or "channel" not in backlog.columns:
        print("  [TODO] margin_backlog missing channel data — Sheet 9 skipped")
        return None

    bl = backlog.dropna(subset=["channel", "harga_jual"]).copy()
    if bl.empty:
        print("  [TODO] margin_backlog has no valid channel+harga_jual rows — Sheet 9 skipped")
        return None

    # Join cost from final_output
    cost_map = (
        ef.groupby("iccid")
        .agg(real_cost_idr=("real_cost_idr", "sum"),
             avg_usage_pct=("usage_pct",     "mean"))
        .reset_index()
    )
    bl = bl.merge(cost_map, on="iccid", how="left")
    bl["real_cost_idr"] = bl["real_cost_idr"].fillna(0)

    # Commission
    if _HAS_COMMISSION:
        bl["commission_rate"] = bl["channel"].apply(get_commission)
    else:
        bl["commission_rate"] = 0.0
        print("  [TODO] config.commission unavailable — komisi_idr set to 0 (Sheet 9)")

    bl["komisi_idr"]    = (bl["harga_jual"] * bl["commission_rate"]).round(0)
    bl["net_revenue"]   = (bl["harga_jual"] - bl["komisi_idr"]).round(0)
    bl["margin_idr"]    = (bl["net_revenue"] - bl["real_cost_idr"]).round(0)
    bl["margin_pct"]    = (
        bl["margin_idr"] / bl["net_revenue"].replace(0, np.nan) * 100
    ).round(1)

    agg = (
        bl.groupby("channel")
        .agg(
            iccid_count       =("iccid",         "nunique"),
            total_harga_jual  =("harga_jual",    "sum"),
            total_komisi_idr  =("komisi_idr",    "sum"),
            total_net_revenue =("net_revenue",   "sum"),
            total_cost_idr    =("real_cost_idr", "sum"),
            total_margin_idr  =("margin_idr",    "sum"),
            avg_margin_pct    =("margin_pct",    "mean"),
            avg_usage_pct     =("avg_usage_pct", "mean"),
        )
        .reset_index()
    )

    for col in ["total_harga_jual","total_komisi_idr","total_net_revenue",
                "total_cost_idr","total_margin_idr"]:
        agg[col] = agg[col].round(0)
    agg["avg_margin_pct"] = agg["avg_margin_pct"].round(1)
    agg["avg_usage_pct"]  = agg["avg_usage_pct"].round(1)

    return (
        agg.sort_values("total_margin_idr", ascending=False)
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 10 — Country × SKU Matrix
# ════════════════════════════════════════════════════════════════════════════

def build_country_sku_matrix(
    ef: pd.DataFrame,
    country: pd.DataFrame,
) -> pd.DataFrame:
    """
    Top 10 countries × Top 10 packages (as SKU proxy), flat format.
    One row per (country_name, package).
    """
    # Top 10 countries by total_usage_gb
    cu = _country_cost_dist(ef, country)
    country_totals = (
        cu.groupby("country_name")["country_usage_mb"].sum()
        .nlargest(10)
        .reset_index(name="total_mb")
    )
    top_countries = country_totals["country_name"].tolist()

    # Top 10 SKUs by iccid_count
    sku_counts = (
        ef.groupby("package")["iccid"].nunique()
        .nlargest(10)
        .reset_index(name="cnt")
    )
    top_skus = sku_counts["package"].tolist()

    # Filter
    cu_f = cu[cu["country_name"].isin(top_countries)].copy()
    cu_f = cu_f.merge(ef[["iccid","package","usage_pct","total_usage_mb"]].drop_duplicates("iccid"),
                      on="iccid", how="left")
    cu_f = cu_f[cu_f["package"].isin(top_skus)]

    agg = (
        cu_f.groupby(["country_name", "package"])
        .agg(
            iccid_count  =("iccid",          "nunique"),
            avg_usage_pct=("usage_pct",      "mean"),
            total_usage_gb_raw=("country_usage_mb","sum"),
        )
        .reset_index()
    )
    agg["total_usage_gb"] = agg["total_usage_gb_raw"].apply(_mb_to_gb)
    agg["avg_usage_pct"]  = agg["avg_usage_pct"].round(1)

    # Grand totals row
    grand = pd.DataFrame([{
        "country_name":  "TOTAL",
        "package":       "TOTAL",
        "iccid_count":   agg["iccid_count"].sum(),
        "avg_usage_pct": agg["avg_usage_pct"].mean().round(1),
        "total_usage_gb": agg["total_usage_gb"].sum().round(2),
    }])

    return (
        pd.concat([
            agg[["country_name","package","iccid_count","avg_usage_pct","total_usage_gb"]]
            .sort_values(["country_name","iccid_count"], ascending=[True, False]),
            grand,
        ], ignore_index=True)
        .rename(columns={"package": "sku"})
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# SHEET 11 — Churn Risk & Anomaly
# ════════════════════════════════════════════════════════════════════════════

def build_churn_anomaly(ef: pd.DataFrame, country: pd.DataFrame) -> pd.DataFrame:
    """
    One row per ICCID with flags and estimated waste.
    Flags (comma-separated): EXPIRED_UNUSED, EXPIRING_SOON, ANOMALY_HIGH,
                              UNDERUTILIZED, GOOD
    """
    top_map = _build_top_country_map(country, n=2)

    iccid_agg = (
        ef.groupby("iccid")
        .agg(
            package         =("package",         "first"),
            duration_segment=("duration_segment","first"),
            quota_tier      =("quota_tier",      "first"),
            end_date        =("end_date",        "max"),
            total_usage_mb  =("total_usage_mb",  "sum"),
            total_quota_mb  =("total_quota_mb",  "sum"),
            real_cost_idr   =("real_cost_idr",   "sum"),
            usage_pct       =("usage_pct",       "mean"),
        )
        .reset_index()
    )

    iccid_agg["total_usage_gb"]  = iccid_agg["total_usage_mb"].apply(_mb_to_gb)
    iccid_agg["total_quota_gb"]  = iccid_agg["total_quota_mb"].apply(_mb_to_gb)
    iccid_agg["usage_pct"]       = iccid_agg["usage_pct"].round(1)
    iccid_agg["days_remaining"]  = (iccid_agg["end_date"] - TODAY).dt.days
    iccid_agg["real_cost_idr"]   = iccid_agg["real_cost_idr"].round(0)
    iccid_agg["estimated_waste_idr"] = (
        (1 - iccid_agg["usage_pct"] / 100).clip(lower=0) * iccid_agg["real_cost_idr"]
    ).round(0)

    def _flags(row):
        flags = []
        pct   = row["usage_pct"]
        rem   = row["days_remaining"]
        is_expired = rem < 0

        if is_expired and pct < 10:
            flags.append("EXPIRED_UNUSED")
        if not is_expired and 0 <= rem <= 3 and pct < 30:
            flags.append("EXPIRING_SOON")
        if pct > 95:
            flags.append("ANOMALY_HIGH")
        if not is_expired and pct < 20:
            flags.append("UNDERUTILIZED")
        if not flags and 30 <= pct <= 80:
            flags.append("GOOD")
        return ", ".join(flags) if flags else "NORMAL"

    iccid_agg["flag"] = iccid_agg.apply(_flags, axis=1)
    iccid_agg = iccid_agg.merge(top_map, on="iccid", how="left")
    iccid_agg["top_countries"] = iccid_agg["top_countries"].fillna("")

    cols = [
        "iccid", "package", "duration_segment", "quota_tier",
        "usage_pct", "days_remaining",
        "total_usage_gb", "total_quota_gb",
        "real_cost_idr", "estimated_waste_idr",
        "top_countries", "flag",
    ]
    return (
        iccid_agg[cols]
        .sort_values("estimated_waste_idr", ascending=False)
        .reset_index(drop=True)
    )


# ════════════════════════════════════════════════════════════════════════════
# WRITER
# ════════════════════════════════════════════════════════════════════════════

_SHEET_META = [
    ("s01_executive",   "1. Executive Summary"),
    ("s02_by_country",  "2. By Country"),
    ("s03_duration",    "3. By Duration"),
    ("s04_quota_tier",  "4. By Quota Tier"),
    ("s05_dur_x_quota", "5. Duration x Quota"),
    ("s06_top_bottom",  "6. Top & Bottom"),
    ("s07_country_perf","7. Country Performance"),
    ("s08_sku_profit",  "8. SKU Profitability"),
    ("s09_channel",     "9. Channel Performance"),
    ("s10_country_sku", "10. Country x SKU"),
    ("s11_churn",       "11. Churn & Anomaly"),
]


def _write_outputs(sections: dict, ts: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # CSV per sheet
    for (csv_key, _), (key, df) in zip(_SHEET_META, sections.items()):
        if df is None:
            print(f"  CSV: {csv_key} — SKIPPED (no data)")
            continue
        path = OUTPUT_DIR / f"looker_{csv_key}_{ts}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  CSV: {path.name}  ({len(df)} rows)")

    # Excel all-in-one
    xlsx_path = OUTPUT_DIR / f"looker_{ts}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for (_, sheet_name), (key, df) in zip(_SHEET_META, sections.items()):
            if df is None:
                continue
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"  XLS: {xlsx_path.name}")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def export_looker(engine=None):
    if engine is None:
        from config.database import get_engine
        engine = get_engine()

    ts = datetime.now().strftime("%Y%m%d_%H%M")

    print("Loading data...")
    final   = _load_final(engine)
    country = _load_country(engine)
    backlog = _load_backlog(engine)
    print(f"  final_output:   {len(final)} rows")
    print(f"  country_usage:  {len(country)} rows")
    print(f"  margin_backlog: {len(backlog)} rows")

    print("\nEnriching final_output...")
    ef = _enrich_final(final)

    print("\nBuilding sheets...")
    by_country = build_by_country(final, country)
    ch9        = build_channel_performance(ef, backlog)

    sections = {
        "s01": build_executive_summary(ef),
        "s02": by_country,
        "s03": build_by_duration(ef, country),
        "s04": build_by_quota_tier(ef),
        "s05": build_duration_quota_matrix(ef),
        "s06": build_top_bottom(ef, country),
        "s07": by_country,                                  # Sheet 7 = Sheet 2
        "s08": build_sku_profitability(ef, backlog, country),
        "s09": ch9,
        "s10": build_country_sku_matrix(ef, country),
        "s11": build_churn_anomaly(ef, country),
    }

    print("\nWriting output...")
    _write_outputs(sections, ts)

    # ── Terminal alerts ─────────────────────────────────────────────────
    s11 = sections["s11"]
    if s11 is not None and not s11.empty:
        waste_total     = s11["estimated_waste_idr"].sum()
        expiring_soon   = s11["flag"].str.contains("EXPIRING_SOON").sum()
        anomaly_high    = s11["flag"].str.contains("ANOMALY_HIGH").sum()
        print(f"\n{'─'*55}")
        print(f"  TOTAL ESTIMATED WASTE:  Rp {waste_total:,.0f}")
        print(f"  EXPIRING SOON (<3 days, <30% used): {expiring_soon} ICCIDs")
        print(f"  ANOMALY HIGH (>95% usage):           {anomaly_high} ICCIDs")
        print(f"{'─'*55}")

    skipped = [m[0] for m, (k, v) in zip(_SHEET_META, sections.items()) if v is None]
    if skipped:
        print(f"\n  Sheets skipped: {', '.join(skipped)}")

    print(f"\nDone → {OUTPUT_DIR}")
    return sections


if __name__ == "__main__":
    export_looker()
