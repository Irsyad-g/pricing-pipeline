import pandas as pd
import numpy as np
import json
import re
from pathlib import Path
from config.database import get_engine

RATE              = 2450
MCC_MAP_PATH      = Path("data/mappings/mcc_map.json")
COUNTRY_RATE_PATH = Path("data/mappings/country_rate.json")
OUTPUT            = Path("data/output/profitability_report.xlsx")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with open(MCC_MAP_PATH)      as f: MCC_MAP      = json.load(f)
with open(COUNTRY_RATE_PATH) as f: COUNTRY_RATE = json.load(f)

GLOBAL_RATE = np.median(list(COUNTRY_RATE.values()))


def extract_mcc(country_str):
    m = re.match(r"(\d+)", str(country_str).strip())
    return m.group(1) if m else None


def get_rate(country_str):
    mcc          = extract_mcc(country_str)
    country_name = MCC_MAP.get(str(mcc)) if mcc else None
    rate         = COUNTRY_RATE.get(country_name) if country_name else None
    return rate if rate else GLOBAL_RATE


def load_data(engine):
    with engine.connect() as conn:
        country_usage = pd.read_sql("""
            SELECT iccid, package, country, month,
                   SUM(country_usage_mb) as usage_mb
            FROM processed.country_usage
            GROUP BY iccid, package, country, month
        """, conn)

        final = pd.read_sql("""
            SELECT iccid, package, month,
                   total_quota_mb, total_usage_mb,
                   usage_ratio, actual_days,
                   start_date, end_date
            FROM processed.final_output
        """, conn)

    return country_usage, final


def extract_country_code_from_package(package):
    from processors.behaviour_factor import extract_country_code
    return extract_country_code(package)


def compute_profitability(country_usage, final):
    country_usage = country_usage.copy()
    country_usage["Rate_CNY"]     = country_usage["country"].apply(get_rate)
    country_usage["Cost_CNY"]     = country_usage["usage_mb"] / 1024 * country_usage["Rate_CNY"]
    country_usage["Country_Name"] = country_usage["country"].apply(
        lambda x: MCC_MAP.get(str(extract_mcc(x)), x)
    )

    cost_actual = (
        country_usage
        .groupby(["iccid", "package", "month"])
        .agg(
            Cost_Aktual_CNY   = ("Cost_CNY",    "sum"),
            Total_Usage_MB    = ("usage_mb",    "sum"),
            Negara_Dikunjungi = ("Country_Name", lambda x: ", ".join(x.dropna().unique()))
        )
        .reset_index()
    )

    pkg_modal = (
        country_usage
        .groupby("package")
        .apply(lambda df: (
            df["Cost_CNY"].sum() / (df["usage_mb"].sum() / 1024)
            if df["usage_mb"].sum() > 0 else 0
        ))
        .reset_index()
    )
    pkg_modal.columns = ["package", "Modal_Per_GB"]

    df = final.merge(cost_actual, on=["iccid", "package", "month"], how="left")
    df = df.merge(pkg_modal, on="package", how="left")

    df["Cost_Aktual_CNY"]   = df["Cost_Aktual_CNY"].fillna(0)
    df["Total_Usage_GB"]    = df["total_usage_mb"].fillna(0) / 1024
    df["Cost_Estimasi_CNY"] = df["Modal_Per_GB"] * df["Total_Usage_GB"]
    df["Selisih_CNY"]       = df["Cost_Aktual_CNY"] - df["Cost_Estimasi_CNY"]
    df["Selisih_IDR"]       = df["Selisih_CNY"] * RATE
    df["Selisih_Pct"]       = np.where(
        df["Cost_Estimasi_CNY"] > 0,
        (df["Selisih_CNY"] / df["Cost_Estimasi_CNY"] * 100).round(2),
        0
    )

    # ekstrak country code sebagai product group
    df["Product_Group"] = df["package"].apply(extract_country_code_from_package)

    return df


def build_summary_package(df):
    summary = (
        df.groupby("package")
        .agg(
            Total_ICCID       = ("iccid",             "nunique"),
            Total_Usage_GB    = ("Total_Usage_GB",    "sum"),
            Cost_Estimasi_CNY = ("Cost_Estimasi_CNY", "sum"),
            Cost_Aktual_CNY   = ("Cost_Aktual_CNY",   "sum"),
            Selisih_CNY       = ("Selisih_CNY",       "sum"),
            Selisih_IDR       = ("Selisih_IDR",       "sum"),
            Avg_Selisih_Pct   = ("Selisih_Pct",       "mean"),
        )
        .reset_index()
    )
    summary["Avg_Selisih_Pct"] = summary["Avg_Selisih_Pct"].round(2)
    summary["Status"] = np.select(
        [summary["Avg_Selisih_Pct"] > 0, summary["Avg_Selisih_Pct"] <= -25],
        ["LOSS", "GAIN"],
        default="NORMAL"
    )
    return summary.sort_values("Selisih_IDR", ascending=False)


def build_summary_month(df):
    summary = (
        df.groupby(["month", "Product_Group"])
        .agg(
            Total_ICCID       = ("iccid",             "nunique"),
            Total_Usage_GB    = ("Total_Usage_GB",    "sum"),
            Cost_Estimasi_CNY = ("Cost_Estimasi_CNY", "sum"),
            Cost_Aktual_CNY   = ("Cost_Aktual_CNY",   "sum"),
            Selisih_CNY       = ("Selisih_CNY",       "sum"),
            Selisih_IDR       = ("Selisih_IDR",       "sum"),
        )
        .reset_index()
    )
    summary["Avg_Selisih_Pct"] = (
        summary["Selisih_CNY"] / summary["Cost_Estimasi_CNY"].replace(0, np.nan) * 100
    ).round(2)
    summary["Status"] = np.select(
        [summary["Avg_Selisih_Pct"] > 1, summary["Avg_Selisih_Pct"] <= -25],
        ["LOSS", "GAIN"],
        default="NORMAL"
    )
    return summary.sort_values(["month", "Product_Group"])


if __name__ == "__main__":
    print("Loading data dari DB...")
    engine = get_engine()
    country_usage, final = load_data(engine)

    print("Menghitung profitability...")
    df           = compute_profitability(country_usage, final)
    summary_pkg  = build_summary_package(df)
    summary_month= build_summary_month(df)

    print("\nSUMMARY PER BULAN:")
    print(summary_month.to_string(index=False))

    # round semua kolom numerik 2 desimal
    for col in ["Cost_Aktual_CNY", "Cost_Estimasi_CNY", "Selisih_CNY",
                "Selisih_IDR", "Selisih_Pct", "Total_Usage_GB"]:
        if col in df.columns:
            df[col] = df[col].round(2)

    summary_pkg   = summary_pkg.round(2)
    summary_month = summary_month.round(2)

    print(f"\nSaving to {OUTPUT}...")
    try:
        with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
            detail_cols = [
                "iccid", "package", "month", "start_date", "end_date",
                "total_quota_mb", "total_usage_mb", "usage_ratio", "actual_days",
                "Negara_Dikunjungi", "Cost_Estimasi_CNY", "Cost_Aktual_CNY",
                "Selisih_CNY", "Selisih_IDR", "Selisih_Pct", "Product_Group"
            ]
            print(f"Kolom tidak ada: {[c for c in detail_cols if c not in df.columns]}")
            df[detail_cols].to_excel(writer, sheet_name="Detail per ICCID",   index=False)
            summary_pkg.to_excel(   writer, sheet_name="Summary per Package", index=False)
            summary_month.to_excel( writer, sheet_name="Summary per Bulan",   index=False)
    except Exception as e:
        import traceback
        traceback.print_exc()

    print("Report selesai!")