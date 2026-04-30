import pandas as pd
import re
import numpy as np

from processors.behaviour_factor import resolve_group
from rules.quota_rules import extract_quota


def _resolve_type(package):
    pkg = str(package).upper()
    if "PURE UNLIMITED" in pkg:
        return "PURE UNLIMITED"
    elif "UNLIMITED" in pkg:
        return "FUP"
    else:
        return "BIG DATA"


def process_subscription(sub, daily):
    """
    Process subscription data.

    Changed from old signature: process_subscription(sub, daily, group_map)
    group_map is no longer needed — resolve_group() reads from country_map.json directly.
    """
    sub = sub.copy()

    sub_expired = sub[sub["STATUS"].str.lower() == "expired"].copy()
    print(f"  Processing {len(sub_expired)} expired subscriptions")

    sub_expired["Group"] = sub_expired["PACKAGE"].apply(resolve_group)
    sub_expired["Type"]  = sub_expired["PACKAGE"].apply(_resolve_type)

    # fix overlap topup di hari yang sama
    sub_expired = sub_expired.sort_values(["ICCID", "START"]).copy()
    sub_expired["NEXT_START"] = sub_expired.groupby("ICCID", observed=True)["START"].shift(-1)
    sub_expired["END"] = np.where(
        sub_expired["NEXT_START"].notna() & (sub_expired["END"] >= sub_expired["NEXT_START"]),
        sub_expired["NEXT_START"] - pd.Timedelta(days=1),
        sub_expired["END"]
    )
    sub_expired = sub_expired.drop(columns=["NEXT_START"])

    sub_expired["TOTAL_QUOTA_MB"] = sub_expired.apply(
        lambda r: extract_quota(r["PACKAGE"], r["DAYS"]), axis=1
    )

    # merge daily ke sub expired
    sub_expired["ICCID"] = sub_expired["ICCID"].astype(str)
    daily_merge           = daily.copy()
    daily_merge["ICCID"]  = daily_merge["ICCID"].astype(str)

    merged = daily_merge.merge(
        sub_expired[["ICCID", "START", "END", "PACKAGE", "Group", "Type", "TOTAL_QUOTA_MB"]],
        on="ICCID",
        how="inner"
    )
    merged = merged[
        (merged["DATE"] >= merged["START"]) &
        (merged["DATE"] <= merged["END"])
    ]
    merged["Month"] = merged["DATE"].dt.strftime("%Y-%b")

    # country usage
    country_df = (
        merged
        .groupby(
            ["ICCID", "PACKAGE", "Group", "TOTAL_QUOTA_MB", "AREA", "Month"],
            dropna=True
        )["USAGE_MB"]
        .sum()
        .reset_index()
        .rename(columns={
            "PACKAGE":        "Package",
            "AREA":           "Country",
            "USAGE_MB":       "Country Usage (MB)",
            "TOTAL_QUOTA_MB": "Total Quota (MB)"
        })
    )

    # final summary
    usage_agg = (
        merged
        .groupby(["ICCID", "PACKAGE", "START"])
        .agg(
            total_usage = ("USAGE_MB",  "sum"),
            actual_days = ("DATE_ONLY", "nunique"),
            visit_area  = ("AREA", lambda x: ", ".join(x.dropna().unique()))
        )
        .reset_index()
    )

    final = sub_expired.merge(usage_agg, on=["ICCID", "PACKAGE", "START"], how="left")
    final["total_usage"] = final["total_usage"].fillna(0).round(2)
    final["actual_days"] = final["actual_days"].fillna(0).astype(int)
    final["visit_area"]  = final["visit_area"].fillna("")

    final["Usage Ratio"] = (
        final["total_usage"] / final["TOTAL_QUOTA_MB"].replace(0, np.nan)
    ).fillna(0)

    ratio_cap = np.select(
        [final["Type"] == "BIG DATA", final["Type"] == "FUP", final["Type"] == "PURE UNLIMITED"],
        [1, 2, 3],
        default=1
    )
    final["Usage Ratio"] = np.minimum(final["Usage Ratio"], ratio_cap).round(2)
    final["Month"] = final["START"].apply(
        lambda x: x.strftime("%Y-%b") if pd.notna(x) else ""
    )

    final = final.rename(columns={
        "PACKAGE":        "Package",
        "START":          "Start",
        "END":            "End",
        "TOTAL_QUOTA_MB": "Total Quota (MB)",
        "total_usage":    "Total Usage (MB)",
        "actual_days":    "Actual Days",
        "visit_area":     "Visit Area",
    })

    final = final[[
        "Type", "Group", "Package", "ICCID", "Month",
        "Start", "End",
        "Total Quota (MB)", "Total Usage (MB)",
        "Usage Ratio", "Actual Days", "Visit Area"
    ]].reset_index(drop=True)

    return final, country_df