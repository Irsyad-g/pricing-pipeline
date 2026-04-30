import pandas as pd
import numpy as np
import json
import re
from pathlib import Path

MCC_MAP_PATH      = Path("data/mappings/mcc_map.json")
COUNTRY_RATE_PATH = Path("data/mappings/country_rate.json")
SKU_FALLBACK_PATH = Path("data/mappings/sku_fallback.json")
RATE              = 2650

with open(MCC_MAP_PATH)      as f: MCC_MAP      = json.load(f)
with open(COUNTRY_RATE_PATH) as f: COUNTRY_RATE = json.load(f)
with open(SKU_FALLBACK_PATH) as f: SKU_FALLBACK = json.load(f)

GLOBAL_RATE = np.median(list(COUNTRY_RATE.values()))


def apply_sku_fallback(sku):
    parts = sku.split("-")
    if len(parts) < 4:
        return sku
    country = parts[1]
    if country in SKU_FALLBACK:
        parts[1] = SKU_FALLBACK[country]
        return "-".join(parts)
    return sku


def extract_mcc(country_str):
    m = re.match(r"(\d+)", str(country_str).strip())
    return m.group(1) if m else None


def get_rate(country_str):
    mcc          = extract_mcc(country_str)
    country_name = MCC_MAP.get(str(mcc)) if mcc else None
    rate         = COUNTRY_RATE.get(country_name) if country_name else None
    return rate if rate else GLOBAL_RATE


def calculate_cost_per_subscription(final_df, daily_df):
    merged = daily_df.merge(
        final_df[["ICCID", "Start", "End", "Package"]],
        on="ICCID",
        how="inner"
    )
    merged = merged[
        (merged["DATE"] >= merged["Start"]) &
        (merged["DATE"] <= merged["End"])
    ]
    merged["Rate_CNY"] = merged["AREA"].apply(get_rate)
    merged["Cost_CNY"] = merged["USAGE_MB"] / 1024 * merged["Rate_CNY"]

    cost = (
        merged
        .groupby(["ICCID", "Start"])
        .agg(Real_Cost_CNY=("Cost_CNY", "sum"))
        .reset_index()
    )
    cost["Real_Cost_IDR"] = (cost["Real_Cost_CNY"] * RATE).round(2)
    cost["Real_Cost_CNY"] = cost["Real_Cost_CNY"].round(4)
    return cost


def _normalize_sku_for_compare(sku):
    """Normalize SKU untuk perbandingan: uppercase, strip whitespace."""
    if not sku or not isinstance(sku, str):
        return ""
    return sku.strip().upper()


def _parse_sku_parts(sku):
    """Parse SKU into components: prefix, country, days, quota."""
    parts = str(sku).upper().split("-")
    if len(parts) < 4:
        return {"raw": sku, "prefix": parts[0] if parts else "",
                "country": "", "days": "", "quota": ""}
    return {
        "raw": sku,
        "prefix": parts[0],
        "country": parts[1],
        "days": parts[2],
        "quota": "-".join(parts[3:]),  # handle quota with dashes
    }


def _build_diagnostic(order_row, final_df, active_sub_df, reason):
    """Build diagnostic info untuk ICCID yang gagal match."""
    iccid = order_row["ICCID"]
    sku   = order_row["SKU"]

    diag = {
        "ICCID": iccid,
        "Order_SKU": sku,
        "Order_SKU_Parts": str(_parse_sku_parts(sku)),
        "Reason": reason,
        "DB_Packages": "",
        "DB_SKUs_Built": "",
        "DB_ICCID_Exists": False,
        "Active_Sub_Exists": False,
        "SKU_Mismatch_Detail": "",
    }

    # cek apakah ICCID ada di final_output
    final_matches = final_df[final_df["iccid"] == iccid]
    if not final_matches.empty:
        diag["DB_ICCID_Exists"] = True
        diag["DB_Packages"] = " | ".join(final_matches["package"].unique()[:5])
        diag["DB_SKUs_Built"] = " | ".join(final_matches["sku_match"].unique()[:5])

        # detail mismatch per component
        order_parts = _parse_sku_parts(sku)
        mismatches = []
        for _, frow in final_matches.iterrows():
            db_parts = _parse_sku_parts(frow["sku_match"])
            diffs = []
            for key in ["country", "days", "quota"]:
                if order_parts[key] != db_parts[key]:
                    diffs.append(f"{key}: order={order_parts[key]} vs db={db_parts[key]}")
            if diffs:
                mismatches.append(f"[{frow['sku_match']}] {', '.join(diffs)}")
        diag["SKU_Mismatch_Detail"] = " || ".join(mismatches[:3])
    else:
        diag["SKU_Mismatch_Detail"] = "ICCID tidak ditemukan di final_output"

    # cek active subscriptions
    if active_sub_df is not None and not active_sub_df.empty:
        active_matches = active_sub_df[active_sub_df["iccid"] == iccid]
        if not active_matches.empty:
            diag["Active_Sub_Exists"] = True

    return diag


def match_orders_to_subscriptions(orders_df, final_df, active_sub_df=None):
    orders = orders_df.copy()
    final  = final_df.copy()

    from processors.behaviour_factor import build_sku
    final["sku_match"] = final["package"].apply(lambda p: build_sku(p).upper())

    # build active SKU lookup
    active_sku_set = set()
    if active_sub_df is not None and not active_sub_df.empty:
        active_sub_df = active_sub_df.copy()
        active_sub_df["sku_match"] = active_sub_df["package"].apply(
            lambda p: build_sku(p).upper()
        )
        active_sku_set = set(
            zip(active_sub_df["iccid"], active_sub_df["sku_match"])
        )

    # normalize SKU untuk matching
    orders["SKU"] = orders["SKU"].apply(_normalize_sku_for_compare)
    final["sku_match"] = final["sku_match"].apply(_normalize_sku_for_compare)

    # ── ROUND 1: match by ICCID + SKU ────────────────────────
    merged = orders.merge(
        final[["iccid", "sku_match", "start_date", "end_date",
               "real_cost_cny", "real_cost_idr",
               "total_usage_mb", "total_quota_mb", "package"]],
        left_on=["ICCID", "SKU"],
        right_on=["iccid", "sku_match"],
        how="left"
    ).drop(columns=["iccid", "sku_match"])
    merged["Match_Method"] = None

    # ── ROUND 2: fallback SKU untuk yang belum match ──────────
    no_match_mask = merged["real_cost_idr"].isna()
    if no_match_mask.any():
        orders_nm = merged.loc[no_match_mask, orders.columns.tolist()].copy()
        orders_nm["SKU_FALLBACK"] = orders_nm["SKU"].apply(apply_sku_fallback)

        changed = orders_nm["SKU_FALLBACK"] != orders_nm["SKU"]
        if changed.any():
            orders_fb = orders_nm[changed].copy()
            merged_fb = orders_fb.merge(
                final[["iccid", "sku_match", "start_date", "end_date",
                       "real_cost_cny", "real_cost_idr",
                       "total_usage_mb", "total_quota_mb", "package"]],
                left_on=["ICCID", "SKU_FALLBACK"],
                right_on=["iccid", "sku_match"],
                how="inner"
            ).drop(columns=["iccid", "sku_match", "SKU_FALLBACK"])
            merged_fb["Match_Method"] = "SKU_FALLBACK"

            if not merged_fb.empty:
                fb_keys = set(zip(merged_fb["Invoice"], merged_fb["ICCID"]))
                merged = merged[~merged.apply(
                    lambda r: (r["Invoice"], r["ICCID"]) in fb_keys, axis=1
                )]
                merged = pd.concat([merged, merged_fb], ignore_index=True)

    # ── ROUND 3: match by ICCID + country code (relaxed) ─────
    no_match_mask2 = merged["real_cost_idr"].isna()
    if no_match_mask2.any():
        orders_nm2 = merged.loc[no_match_mask2, orders.columns.tolist()].copy()
        orders_nm2["_country"] = orders_nm2["SKU"].apply(
            lambda s: _parse_sku_parts(s)["country"]
        )
        final["_country"] = final["sku_match"].apply(
            lambda s: _parse_sku_parts(s)["country"]
        )

        merged_r3 = orders_nm2.merge(
            final[["iccid", "_country", "sku_match", "start_date", "end_date",
                   "real_cost_cny", "real_cost_idr",
                   "total_usage_mb", "total_quota_mb", "package"]],
            left_on=["ICCID", "_country"],
            right_on=["iccid", "_country"],
            how="inner"
        ).drop(columns=["iccid", "_country"])
        merged_r3["Match_Method"] = "COUNTRY_MATCH"

        if not merged_r3.empty:
            r3_keys = set(zip(merged_r3["Invoice"], merged_r3["ICCID"]))
            merged = merged[~merged.apply(
                lambda r: (r["Invoice"], r["ICCID"]) in r3_keys, axis=1
            )]
            merged = pd.concat([merged, merged_r3], ignore_index=True)

        # cleanup temp column
        final.drop(columns=["_country"], inplace=True, errors="ignore")

    # ── SORT & DEDUP ──────────────────────────────────────────
    merged["start_date"]  = pd.to_datetime(merged["start_date"],  errors="coerce")
    merged["Order_Date"]  = pd.to_datetime(merged["Order_Date"],  errors="coerce")
    merged["date_diff"]   = (merged["start_date"] - merged["Order_Date"]).dt.days.abs()
    merged["after_order"] = (merged["start_date"] >= merged["Order_Date"]).astype(int)
    merged = merged.sort_values(
        ["ICCID", "Invoice", "after_order", "date_diff"],
        ascending=[True, True, False, True]
    )
    matched = merged.drop_duplicates(subset=["Invoice", "ICCID"]).drop(
        columns=["date_diff", "after_order"]
    )

    # ── ASSIGN MATCH METHOD ───────────────────────────────────
    def get_reason(row):
        if row["Match_Method"] == "SKU_FALLBACK":
            return "SKU_FALLBACK"
        if row["Match_Method"] == "COUNTRY_MATCH":
            return "COUNTRY_MATCH"
        if pd.notna(row["real_cost_idr"]):
            return "SKU_MATCH"
        if (row["ICCID"], row["SKU"]) in active_sku_set:
            return "ACTIVE"
        sku_fb = apply_sku_fallback(row["SKU"])
        if (row["ICCID"], sku_fb) in active_sku_set:
            return "ACTIVE"
        return "NO_ORDER_DATA"

    matched["Match_Method"] = matched.apply(get_reason, axis=1)

    # ── DIAGNOSTICS untuk yang gagal match ────────────────────
    diagnostics = []
    failed = matched[matched["Match_Method"].isin(["NO_ORDER_DATA", "ACTIVE"])]
    for _, row in failed.iterrows():
        diag = _build_diagnostic(row, final, active_sub_df, row["Match_Method"])
        diagnostics.append(diag)

    diagnostics_df = pd.DataFrame(diagnostics) if diagnostics else pd.DataFrame()

    # ── SUMMARY ───────────────────────────────────────────────
    ok       = (matched["Match_Method"] == "SKU_MATCH").sum()
    fallback = (matched["Match_Method"] == "SKU_FALLBACK").sum()
    country  = (matched["Match_Method"] == "COUNTRY_MATCH").sum()
    active   = (matched["Match_Method"] == "ACTIVE").sum()
    no_data  = (matched["Match_Method"] == "NO_ORDER_DATA").sum()
    print(f"  SKU match: {ok} | Fallback: {fallback} | Country: {country} "
          f"| Masih aktif: {active} | Tidak ada data: {no_data}")

    # drop temp column
    matched.drop(columns=["package"], inplace=True, errors="ignore")

    return matched, diagnostics_df