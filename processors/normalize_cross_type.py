"""
Cross-Type Normalization + Stacking Layer (v3)
===============================================
Memastikan:
1. STACKING: setiap kenaikan kuota WAJIB ada kenaikan harga (min 10rb)
2. Big Data ≥ FUP untuk total GB equivalent (matching by total GB, not daily rate)
3. FUP < BD untuk exact same total GB (BD = guaranteed quota = premium)

Urutan di export_pricing():
    df = calculate_cost_opportunity(df)
    df = normalize_cross_type(df)        # ← INI
    df = apply_premium_pricing(df)
"""

import math
import pandas as pd


# =====================
# CONFIG
# =====================
FUP_QUOTA_GB = {
    "500": 0.5, "800": 0.8, "1": 1.0, "15": 1.5,
    "2": 2.0, "3": 3.0, "5": 5.0
}

FUP_ORDER = ["500", "800", "1", "15", "2", "3", "5"]

# Minimum price increment per tier step
MIN_FUP_STEP = 10000       # FUP: min 10rb antar tier
MIN_BD_STEP  = 10000       # BD < 20GB: min 10rb antar tier


def round_9000(price):
    return price - (price % 10000) + 9000


def _stack_fup(df, fup_mask, price_cols):
    """Phase 1: enforce FUP tier monotonicity with min step."""
    fixes = 0
    for (negara, hari), group_idx in df[fup_mask].groupby(["NEGARA", "HARI"]).groups.items():
        group = df.loc[group_idx].copy()
        group["_tier_idx"] = group["KUOTA"].apply(
            lambda q: FUP_ORDER.index(str(q)) if str(q) in FUP_ORDER else -1
        )
        group = group[group["_tier_idx"] >= 0].sort_values("_tier_idx")
        if len(group) <= 1:
            continue

        indices = group.index.tolist()
        for col in price_cols:
            for i in range(1, len(indices)):
                prev_price = df.at[indices[i-1], col]
                min_price = prev_price + MIN_FUP_STEP
                if df.at[indices[i], col] < min_price:
                    df.at[indices[i], col] = round_9000(min_price)
                    if col == "HARGA_FLAT":
                        fixes += 1
    return fixes


def _stack_bd(df, bd_mask, price_cols):
    """Phase 1b: enforce BD tier monotonicity with scaled min step."""
    fixes = 0
    for (negara, hari), group_idx in df[bd_mask].groupby(["NEGARA", "HARI"]).groups.items():
        group = df.loc[group_idx].copy()
        group["_bd_gb"] = group["KUOTA"].apply(
            lambda q: float(str(q).upper().replace("GB", "")) if "GB" in str(q).upper() else 0
        )
        group = group.sort_values("_bd_gb")
        if len(group) <= 1:
            continue

        indices = group.index.tolist()
        gb_vals = group["_bd_gb"].values

        for col in price_cols:
            for i in range(1, len(indices)):
                gb_diff = gb_vals[i] - gb_vals[i-1]
                step = max(MIN_BD_STEP, MIN_BD_STEP * (gb_diff / 2))
                min_price = df.at[indices[i-1], col] + step
                if df.at[indices[i], col] < min_price:
                    df.at[indices[i], col] = round_9000(min_price)
                    if col == "HARGA_FLAT":
                        fixes += 1
    return fixes


def normalize_cross_type(df):
    """
    Phase 1:  FUP stacking (min 10rb antar tier)
    Phase 1b: BD stacking (min 10rb, scaled by GB diff)
    Phase 2:  Cross-type: BD ≥ FUP for same total GB
    Phase 2b: FUP ceiling: FUP < BD for exact same total GB
    Phase 3:  Re-stack setelah cross-type adjustments
    """
    df = df.copy()
    price_cols = ["HARGA_FLAT", "HARGA_SIM", "HARGA_ESIM"]

    fup_mask = df["TYPE"] == "FUP"
    bd_mask  = df["TYPE"] == "BIG DATA"

    # ── PHASE 1: STACKING ──────────────────────────────
    fup_stacks = _stack_fup(df, fup_mask, price_cols)
    bd_stacks  = _stack_bd(df, bd_mask, price_cols)

    # ── PHASE 2: BD ≥ FUP (total GB based) ─────────────
    # For each BD SKU, find FUP tier where daily_rate ≤ BD_GB/hari
    # BD must be ≥ that FUP + step

    fup_price_lookup = {}
    for idx in df[fup_mask].index:
        row = df.loc[idx]
        key = (row["NEGARA"], row["HARI"], str(row["KUOTA"]))
        fup_price_lookup[key] = {col: df.at[idx, col] for col in price_cols}

    bd_raised = 0
    for idx in df[bd_mask].index:
        row = df.loc[idx]
        negara, hari = row["NEGARA"], row["HARI"]
        kuota_str = str(row["KUOTA"]).upper()
        if "GB" not in kuota_str:
            continue
        bd_gb = float(kuota_str.replace("GB", ""))
        if bd_gb <= 0 or hari <= 0:
            continue

        gb_per_day = bd_gb / hari

        # Find closest FUP tier ≤ gb_per_day
        best_fup = None
        for fq, fgb in FUP_QUOTA_GB.items():
            if fgb <= gb_per_day + 0.01:
                if best_fup is None or fgb > FUP_QUOTA_GB[best_fup]:
                    best_fup = fq

        if best_fup is None:
            best_fup = "500"

        fup_key = (negara, hari, best_fup)
        if fup_key not in fup_price_lookup:
            continue

        for col in price_cols:
            fup_price = fup_price_lookup[fup_key][col]
            bd_floor = round_9000(fup_price + MIN_BD_STEP)
            if df.at[idx, col] < bd_floor:
                df.at[idx, col] = bd_floor
                if col == "HARGA_FLAT":
                    bd_raised += 1

    # ── PHASE 2b: FUP < BD for EXACT same total GB ────
    # Rebuild BD lookup after raises
    bd_price_lookup = {}
    for idx in df[bd_mask].index:
        row = df.loc[idx]
        kuota_str = str(row["KUOTA"]).upper()
        if "GB" not in kuota_str:
            continue
        bd_gb = float(kuota_str.replace("GB", ""))
        key = (row["NEGARA"], row["HARI"], bd_gb)
        bd_price_lookup[key] = {col: df.at[idx, col] for col in price_cols}

    fup_capped = 0
    for idx in df[fup_mask].index:
        row = df.loc[idx]
        negara, hari = row["NEGARA"], row["HARI"]
        kuota = str(row["KUOTA"])
        if kuota not in FUP_QUOTA_GB:
            continue

        total_gb = FUP_QUOTA_GB[kuota] * hari

        # HANYA cek exact match
        bd_key = (negara, hari, total_gb)
        if bd_key not in bd_price_lookup:
            continue

        for col in price_cols:
            bd_price = bd_price_lookup[bd_key][col]
            if df.at[idx, col] >= bd_price:
                # Raise BD instead of capping FUP (to preserve stacking)
                new_bd = round_9000(df.at[idx, col] + MIN_BD_STEP)
                bd_price_lookup[bd_key][col] = new_bd
                # Find and update BD row
                for bd_idx in df[bd_mask].index:
                    bd_row = df.loc[bd_idx]
                    if (bd_row["NEGARA"] == negara and bd_row["HARI"] == hari):
                        bd_kuota = str(bd_row["KUOTA"]).upper()
                        if "GB" in bd_kuota and float(bd_kuota.replace("GB", "")) == total_gb:
                            df.at[bd_idx, col] = new_bd
                            if col == "HARGA_FLAT":
                                fup_capped += 1
                            break

    # ── PHASE 3: RE-STACK ──────────────────────────────
    restack_fup = _stack_fup(df, fup_mask, price_cols)
    restack_bd  = _stack_bd(df, bd_mask, price_cols)
    restack_fixes = restack_fup + restack_bd

    # ── PHASE 4: Recalculate Cost Opportunity ──────────
    if "HARGA_BF_FLAT" in df.columns:
        df["COST_OPP_SIM"]  = (df["HARGA_SIM"]  - df["HARGA_BF_SIM"]).clip(lower=0)
        df["COST_OPP_ESIM"] = (df["HARGA_ESIM"] - df["HARGA_BF_ESIM"]).clip(lower=0)
        df["COST_OPP_FLAT"] = (df["HARGA_FLAT"] - df["HARGA_BF_FLAT"]).clip(lower=0)

    # ── DEBUG ──────────────────────────────────────────
    print(f"\n🔄 CROSS-TYPE NORMALIZATION + STACKING:")
    print(f"  FUP stacking fixes:    {fup_stacks}")
    print(f"  BD stacking fixes:     {bd_stacks}")
    print(f"  BD raised (≥ FUP):     {bd_raised}")
    print(f"  BD raised (exact GB):  {fup_capped}")
    print(f"  Re-stack fixes:        {restack_fixes}")

    # Spot check
    sample_countries = sorted(df["NEGARA"].unique())[:3]
    for neg in sample_countries:
        for hari in [1, 4, 7]:
            fup_sub = df[(df["NEGARA"]==neg) & (df["HARI"]==hari) & (df["TYPE"]=="FUP")]
            if fup_sub.empty:
                continue
            fup_sub = fup_sub.copy()
            fup_sub["_gb"] = fup_sub["KUOTA"].apply(lambda q: FUP_QUOTA_GB.get(str(q), 0))
            fup_sub = fup_sub.sort_values("_gb")
            prices = fup_sub["HARGA_FLAT"].values
            tiers = fup_sub["KUOTA"].values
            is_mono = all(prices[i] > prices[i-1] for i in range(1, len(prices)))
            status = "✅" if is_mono else "❌"
            tier_prices = " → ".join(f"{t}:{int(p):,}" for t, p in zip(tiers, prices))
            print(f"  {status} {neg} {hari}d FUP: {tier_prices}")

    return df