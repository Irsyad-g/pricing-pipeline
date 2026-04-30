import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pathlib import Path
import numpy as np
import json
import re
import math
from processors.normalize_cross_type import normalize_cross_type

# =====================
# CONFIG
# =====================
CONFIG_PATH = Path("config/google_credentials.json")
SHEET_NAME = "NEW PERUBAHAN HARGA SUPPLIER"
MCC_MAP_PATH = Path("data/mappings/mcc_map.json")
COUNTRY_RATE_PATH = Path("data/mappings/country_rate.json")

DEVICE_SIM = 2.72
DEVICE_ESIM = 2.92
RATE = 2650

TARGET_MARGIN = 0.25  # 25% margin target for all types

# =====================
# PREMIUM (MAX) CONFIG
# =====================
# Multiplier: harga MAX = harga BASE (PLUS) × multiplier
# Berlaku generik untuk semua negara *MAX
PREMIUM_MULTIPLIER = 1.35

# Multiplier untuk exclusive quota (misal FUP 10GB/day hanya ada di MAX)
# Diturunkan dari tier tertinggi yang ada di BASE (5GB) × multiplier ini
EXCLUSIVE_QUOTA_MULTIPLIER = 1.50

with open(MCC_MAP_PATH) as f:
    MCC_MAP = json.load(f)
with open(COUNTRY_RATE_PATH) as f:
    COUNTRY_RATE = json.load(f)



# =====================
# CONNECT
# =====================
def connect():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG_PATH, scope)
    client = gspread.authorize(creds)

    return client.open(SHEET_NAME)


# =====================
# READ SHEET
# =====================
def read_sheet(sheet, name):
    ws = sheet.worksheet(name)
    return pd.DataFrame(ws.get_all_records())


# =====================
# TYPE DETECTION
# =====================
def get_type(quota):
    quota = str(quota).upper()
    if "GB" in quota:
        return "BIG DATA"
    elif quota == "UNL":
        return "PURE UNLIMITED"
    else:
        return "FUP"  


# HELPER FUNCTION
def flat_price(cost_idr):
    price = cost_idr + 25000
    return price - (price % 10000)+9000

def round_9000(price):
        return price - (price % 10000) + 9000

def margin_sim(price):
    if price * 0.35 > 35000:
        result = price * 1.35
        return result - (result % 10000) + 9000
    else:
        result = price + 35000
        return result - (result % 10000) + 9000

def margin_esim(price):
    if price * 0.25 > 25000:
        result = price * 1.25
        return result - (result % 10000) + 9000
    else:
        result = price + 25000
        return result - (result % 10000) + 9000


# =====================
# PARSE SKU
# =====================
def parse_sku(df):
    parts = df["SKU"].str.split("-", expand=True)

    df["NEGARA"] = parts[1]
    df["HARI"] = parts[2].astype(int)
    df["KUOTA"] = parts[3]

    return df

FUP_QUOTA_MAP = {
    "500": 0.5,
    "800": 0.8,
    "15":  1.5,
    "1":   1.0,
    "2":   2.0,
    "3":   3.0,
    "5":   5.0
}
BD_GB_COEF  = 0.1016   # GB discount coefficient
BD_DAY_COEF = 0.065    # duration factor coefficient

def get_gb_discount(gb):
    """Makin besar GB, makin rendah expected usage rate."""
    return max(0.5, 1 - BD_GB_COEF * math.log(max(gb, 1) / 3))

def get_duration_factor(day):
    """Gentle logarithmic duration factor."""
    return 1 + BD_DAY_COEF * math.log(max(day, 1))

def parse_quota_value(quota):
    quota = str(quota).upper()

    if quota == "UNL":
        return 10.0

    if "GB" in quota:
        return float(quota.replace("GB", ""))

    # strip MB kalau ada sebelum lookup FUP_QUOTA_MAP
    quota_key = quota.replace("MB", "").strip()

    if quota_key in FUP_QUOTA_MAP:
        return FUP_QUOTA_MAP[quota_key]

    return float(quota_key)  # fallback



# =====================
# FUP TIER ORDER & GAP
# =====================
# Sorted by GB value for gap calculation
FUP_TIER_ORDER = [
    ("500", 0.5),
    ("800", 0.8),
    ("1",   1.0),
    ("15",  1.5),
    ("2",   2.0),
    ("3",   3.0),
    ("5",   5.0),
]

# Base gap per 0.5GB increment (IDR) — scales with duration
FUP_BASE_GAP_PER_HALF_GB = 5000

def get_fup_tier_index(quota_key):
    """Return index in FUP_TIER_ORDER, or -1 if not found."""
    for i, (key, _) in enumerate(FUP_TIER_ORDER):
        if key == str(quota_key):
            return i
    return -1

def get_fup_gb_value(quota_key):
    """Return GB value for FUP quota key."""
    for key, gb in FUP_TIER_ORDER:
        if key == str(quota_key):
            return gb
    return 0


# =====================
# PREMIUM (MAX) PRICING
# =====================
def get_base_country(negara):
    """
    Detect if negara is a premium variant and return base country.
    CHMMAX → CHMPLUS, JPNMAX → JPNPLUS, etc.
    Returns (base_country, is_premium) tuple.
    """
    negara = str(negara).upper()
    if negara.endswith("MAX"):
        base = negara[:-3] + "PLUS"
        return base, True
    return negara, False


def apply_premium_pricing(df):
    """
    Override harga produk MAX berdasarkan harga BASE (PLUS) × PREMIUM_MULTIPLIER.
    
    Rules:
    1. Untuk quota yang ada di BASE: harga MAX = harga BASE × 1.35
    2. Untuk quota exclusive MAX (misal FUP 10GB/day):
       harga = tier tertinggi BASE yang ada di FUP (5GB) × 1.35 × 1.50
    3. Safety: harga MAX selalu >= harga BASE (never cheaper)
    """
    
    
    # Identify premium countries
    df["_BASE_COUNTRY"] = df["NEGARA"].apply(lambda n: get_base_country(n)[0])
    df["_IS_PREMIUM"]   = df["NEGARA"].apply(lambda n: get_base_country(n)[1])
    
    premium_countries = df[df["_IS_PREMIUM"]]["NEGARA"].unique()
    
    if len(premium_countries) == 0:
        df = df.drop(columns=["_BASE_COUNTRY", "_IS_PREMIUM"])
        return df
    
    print(f"\nPREMIUM COUNTRIES DETECTED: {list(premium_countries)}")
    
    # Build lookup: base country prices per (HARI, KUOTA, TYPE)
    base_countries = df[df["_IS_PREMIUM"]]["_BASE_COUNTRY"].unique()
    base_df = df[df["NEGARA"].isin(base_countries)].copy()
    
    base_lookup = base_df.set_index(["NEGARA", "HARI", "KUOTA", "TYPE"])[
        ["HARGA_FLAT", "HARGA_SIM", "HARGA_ESIM"]
    ].to_dict("index")
    
    # Also build FUP max tier lookup per (BASE_COUNTRY, HARI) — for exclusive quotas
    fup_max_tier = {}
    fup_base = base_df[base_df["TYPE"] == "FUP"].copy()
    fup_base["FUP_GB"] = fup_base["KUOTA"].astype(str).apply(get_fup_gb_value)
    
    for (negara, hari), group in fup_base.groupby(["NEGARA", "HARI"]):
        top = group.sort_values("FUP_GB", ascending=False).iloc[0]
        fup_max_tier[(negara, hari)] = {
            "HARGA_FLAT": top["HARGA_FLAT"],
            "HARGA_SIM":  top["HARGA_SIM"],
            "HARGA_ESIM": top["HARGA_ESIM"],
            "KUOTA":      top["KUOTA"],
            "FUP_GB":     top["FUP_GB"],
        }
    
    # Apply premium pricing
    # KEY: use REPLACE (=), not max() — cost opportunity may have inflated 
    # MAX prices far beyond what makes sense relative to PLUS.
    # Premium price = PLUS × 1.35 is the source of truth for MAX.
    price_cols = ["HARGA_FLAT", "HARGA_SIM", "HARGA_ESIM"]
    premium_applied = 0
    exclusive_applied = 0
    
    for idx, row in df[df["_IS_PREMIUM"]].iterrows():
        base_country = row["_BASE_COUNTRY"]
        hari   = row["HARI"]
        kuota  = row["KUOTA"]
        tipe   = row["TYPE"]
        
        base_key = (base_country, hari, kuota, tipe)
        
        if base_key in base_lookup:
            # Normal: quota exists in BASE → REPLACE with BASE × multiplier
            base_prices = base_lookup[base_key]
            for col in price_cols:
                df.at[idx, col] = round_9000(base_prices[col] * PREMIUM_MULTIPLIER)
            premium_applied += 1
        
        elif tipe == "FUP":
            # Exclusive quota → derive from highest FUP tier in BASE
            fup_ref = fup_max_tier.get((base_country, hari))
            if fup_ref:
                for col in price_cols:
                    ref_price = fup_ref[col]
                    df.at[idx, col] = round_9000(ref_price * PREMIUM_MULTIPLIER * EXCLUSIVE_QUOTA_MULTIPLIER)
                exclusive_applied += 1
    
    # Safety check: ensure MAX is never cheaper than BASE for matching SKUs
    safety_fixes = 0
    for idx, row in df[df["_IS_PREMIUM"]].iterrows():
        base_key = (row["_BASE_COUNTRY"], row["HARI"], row["KUOTA"], row["TYPE"])
        if base_key in base_lookup:
            for col in price_cols:
                if df.at[idx, col] < base_lookup[base_key][col]:
                    df.at[idx, col] = round_9000(base_lookup[base_key][col] * 1.10)
                    safety_fixes += 1
    
    print(f"  Premium applied: {premium_applied} SKUs")
    print(f"  Exclusive derived: {exclusive_applied} SKUs")
    print(f"  Safety fixes (MAX < PLUS): {safety_fixes}")
    
    df = df.drop(columns=["_BASE_COUNTRY", "_IS_PREMIUM"])
    return df


# =====================
# COST OPPORTUNITY CALCULATOR
# =====================
def calculate_cost_opportunity(df):
    """
    Calculate Cost Opportunity: minimum price markup to ensure:
    1. Tier gap antar quota yang masuk akal (FUP & Big Data)
    2. Margin minimum 25% dari cost
    3. UNL premium terhadap FUP tertinggi
    
    Returns df with new columns:
    - HARGA_BF_SIM    : harga murni dari behaviour factor (SIM)
    - HARGA_BF_ESIM   : harga murni dari behaviour factor (eSIM)
    - HARGA_BF_FLAT   : harga murni dari behaviour factor (FLAT)
    - COST_OPP_SIM    : cost opportunity markup (SIM)
    - COST_OPP_ESIM   : cost opportunity markup (eSIM)
    - COST_OPP_FLAT   : cost opportunity markup (FLAT)
    """
    
    df = df.copy()
    
    # ---------------------------------------------------
    # STEP 1: Save HARGA_BEHAVIOUR (harga murni dari BF)
    # ---------------------------------------------------
    df["HARGA_BF_SIM"]  = df["HARGA_SIM"].copy()
    df["HARGA_BF_ESIM"] = df["HARGA_ESIM"].copy()
    df["HARGA_BF_FLAT"] = df["HARGA_FLAT"].copy()
    # ---------------------------------------------------
    # STEP 2: Margin Floor — ensure 25% margin from cost
    # ---------------------------------------------------
    # margin_floor_sim  = COST_SIM_IDR / (1 - TARGET_MARGIN)
    # margin_floor_esim = COST_ESIM_IDR / (1 - TARGET_MARGIN)
    # margin_floor_flat = MODAL_ADJ_IDR / (1 - TARGET_MARGIN)
    
    def round_9000(price):
        """Round to nearest 10.000 ending 9.000"""
        return price - (price % 10000) + 9000
    
    df["MARGIN_FLOOR_SIM"]  = df["COST_SIM_IDR"].apply(
        lambda c: round_9000(c / (1 - TARGET_MARGIN))
    )
    df["MARGIN_FLOOR_ESIM"] = df["COST_ESIM_IDR"].apply(
        lambda c: round_9000(c / (1 - TARGET_MARGIN))
    )
    df["MARGIN_FLOOR_FLAT"] = df["MODAL_ADJ_IDR"].apply(
        lambda c: round_9000(c / (1 - TARGET_MARGIN))
    )
    
    # Apply margin floor — harga minimal supaya margin >= 25%
    df["HARGA_SIM"]  = df[["HARGA_SIM",  "MARGIN_FLOOR_SIM"]].max(axis=1)
    df["HARGA_ESIM"] = df[["HARGA_ESIM", "MARGIN_FLOOR_ESIM"]].max(axis=1)
    df["HARGA_FLAT"] = df[["HARGA_FLAT", "MARGIN_FLOOR_FLAT"]].max(axis=1)
    
    # ---------------------------------------------------
    # STEP 3: FUP Tier Gap — enforce minimum gap antar tier
    # ---------------------------------------------------
    # Group by NEGARA + HARI, sort by GB value, enforce cumulative gap
    
    fup_mask = df["TYPE"] == "FUP"
    fup_df = df[fup_mask].copy()
    non_fup_df = df[~fup_mask].copy()
    
    fup_results = []
    
    for (negara, hari), group in fup_df.groupby(["NEGARA", "HARI"]):
        group = group.copy()
        group["FUP_GB"] = group["KUOTA_ORIGINAL"].apply(get_fup_gb_value)
        group = group.sort_values("FUP_GB")
        
        if len(group) <= 1:
            fup_results.append(group)
            continue
        
        # Day scaling: gap grows with duration (log scale)
        day_scale = 1 + 0.15 * math.log(max(hari, 1))
        
        prices_sim  = group["HARGA_SIM"].values.copy()
        prices_esim = group["HARGA_ESIM"].values.copy()
        prices_flat = group["HARGA_FLAT"].values.copy()
        gb_vals     = group["FUP_GB"].values.copy()
        
        for i in range(1, len(gb_vals)):
            gb_diff = gb_vals[i] - gb_vals[i - 1]
            
            # Gap = 15.000 per 0.5GB selisih, scaled by duration
            min_gap = FUP_BASE_GAP_PER_HALF_GB * (gb_diff / 0.5) * day_scale
            
            # Cap: gap tidak boleh lebih dari 40% harga tier sebelumnya
            max_gap_sim  = prices_sim[i - 1]  * 0.40
            max_gap_esim = prices_esim[i - 1] * 0.40
            max_gap_flat = prices_flat[i - 1] * 0.40
            
            effective_gap_sim  = min(min_gap, max_gap_sim)
            effective_gap_esim = min(min_gap, max_gap_esim)
            effective_gap_flat = min(min_gap, max_gap_flat)
            
            min_sim  = prices_sim[i - 1]  + effective_gap_sim
            min_esim = prices_esim[i - 1] + effective_gap_esim
            min_flat = prices_flat[i - 1] + effective_gap_flat
            
            prices_sim[i]  = max(prices_sim[i],  min_sim)
            prices_esim[i] = max(prices_esim[i], min_esim)
            prices_flat[i] = max(prices_flat[i], min_flat)
        
        # Re-apply rounding
        group["HARGA_SIM"]  = [round_9000(p) for p in prices_sim]
        group["HARGA_ESIM"] = [round_9000(p) for p in prices_esim]
        group["HARGA_FLAT"] = [round_9000(p) for p in prices_flat]
        
        fup_results.append(group)
    
    if fup_results:
        fup_df = pd.concat(fup_results).reset_index(drop=True)
    
    # ---------------------------------------------------
    # STEP 4: UNL Premium — min 1.3× dari FUP 3GB/day equivalent
    # ---------------------------------------------------
    # Build reference: harga FUP "3" per (NEGARA, HARI)
    fup_3gb_ref = fup_df[fup_df["KUOTA_ORIGINAL"] == "3"].copy()
    
    if not fup_3gb_ref.empty:
        fup_3gb_lookup = (
            fup_3gb_ref.groupby(["NEGARA", "HARI"])
            [["HARGA_SIM", "HARGA_ESIM", "HARGA_FLAT"]]
            .first()
            .reset_index()
            .rename(columns={
                "HARGA_SIM":  "REF_SIM",
                "HARGA_ESIM": "REF_ESIM",
                "HARGA_FLAT": "REF_FLAT",
            })
        )
        
        unl_mask = non_fup_df["TYPE"] == "PURE UNLIMITED"
        unl_df = non_fup_df[unl_mask].copy()
        bd_df  = non_fup_df[~unl_mask].copy()
        
        if not unl_df.empty:
            unl_df = unl_df.merge(fup_3gb_lookup, on=["NEGARA", "HARI"], how="left")
            
            # UNL minimal 1.3× FUP 3GB/day
            UNL_PREMIUM = 1.3
            for col_price, col_ref in [
                ("HARGA_SIM",  "REF_SIM"),
                ("HARGA_ESIM", "REF_ESIM"),
                ("HARGA_FLAT", "REF_FLAT"),
            ]:
                mask = unl_df[col_ref].notna()
                unl_df.loc[mask, col_price] = unl_df.loc[mask].apply(
                    lambda r: max(r[col_price], round_9000(r[col_ref] * UNL_PREMIUM)),
                    axis=1
                )
            
            # Drop reference columns
            unl_df = unl_df.drop(columns=["REF_SIM", "REF_ESIM", "REF_FLAT"], errors="ignore")
            
            non_fup_df = pd.concat([bd_df, unl_df]).reset_index(drop=True)
    
    # ---------------------------------------------------
    # STEP 5: Recombine & calculate COST_OPPORTUNITY columns
    # ---------------------------------------------------
    df = pd.concat([fup_df, non_fup_df]).reset_index(drop=True)
    
    # Drop temp columns
    df = df.drop(columns=[
        "MARGIN_FLOOR_SIM", "MARGIN_FLOOR_ESIM", "MARGIN_FLOOR_FLAT",
        "FUP_GB"
    ], errors="ignore")
    
    # Cost Opportunity = difference between final price and behaviour price
    df["COST_OPP_SIM"]  = (df["HARGA_SIM"]  - df["HARGA_BF_SIM"]).clip(lower=0)
    df["COST_OPP_ESIM"] = (df["HARGA_ESIM"] - df["HARGA_BF_ESIM"]).clip(lower=0)
    df["COST_OPP_FLAT"] = (df["HARGA_FLAT"] - df["HARGA_BF_FLAT"]).clip(lower=0)
    
    # Actual margin calculation
    df["MARGIN_SIM"]  = ((df["HARGA_SIM"]  - df["COST_SIM_IDR"])  / df["HARGA_SIM"]  * 100).round(1)
    df["MARGIN_ESIM"] = ((df["HARGA_ESIM"] - df["COST_ESIM_IDR"]) / df["HARGA_ESIM"] * 100).round(1)
    df["MARGIN_FLAT"] = ((df["HARGA_FLAT"] - df["MODAL_ADJ_IDR"]) / df["HARGA_FLAT"] * 100).round(1)
    
    return df


# =====================
# MAIN EXPORT
# =====================

def extract_mcc(area):
    match = re.match(r"(\d+)", str(area).strip())
    return match.group(1) if match else None

def calculate_weighted_modal(package, country_usage_df):
    pkg_usage = (
        country_usage_df[country_usage_df["Package"] == package]
        .groupby("Country")["Country Usage (MB)"]
        .sum()
        .reset_index()
    )
    
    if pkg_usage.empty:
        return None

    total_usage = pkg_usage["Country Usage (MB)"].sum()
    if total_usage == 0:
        return None

    pkg_usage["Weight"] = pkg_usage["Country Usage (MB)"] / total_usage
    pkg_usage["MCC"] = pkg_usage["Country"].apply(extract_mcc)
    pkg_usage["Country_Name"] = pkg_usage["MCC"].map(MCC_MAP)
    pkg_usage["Rate_CNY"] = pkg_usage["Country_Name"].map(COUNTRY_RATE)

    missing = pkg_usage[pkg_usage["Rate_CNY"].isna()]
    if not missing.empty:
        print(f"⚠️ RATE MISSING for {package}:")
        print(missing[["Country", "MCC", "Country_Name"]].drop_duplicates())

    pkg_usage["Rate_CNY"] = pkg_usage["Rate_CNY"].fillna(pkg_usage["Rate_CNY"].median())
    weighted_modal = (pkg_usage["Weight"] * pkg_usage["Rate_CNY"]).sum()
    
    return weighted_modal

def smooth_prices(df):
    result = []

    SMOOTH_CAP = {
        "BIG DATA":        None,   
        "FUP":             0.15,   
        "PURE UNLIMITED":  0.12,   
    }

    for (negara, kuota, tipe), group in df.groupby(["NEGARA", "KUOTA", "TYPE"]):
        group = group.sort_values("HARI").copy()
        cap   = SMOOTH_CAP.get(tipe)
        hari_vals = group["HARI"].values.astype(float)

        for col in ["HARGA_SIM", "HARGA_ESIM", "HARGA_FLAT"]:
            prices = group[col].values.copy().astype(float)

            if tipe == "BIG DATA":
                for i in range(1, len(prices)):
                    if prices[i] < prices[i - 1]:
                        prices[i] = prices[i - 1]

            else:
                # Pass 1: fix inversions (day d2 cheaper than day d1 where d2 > d1)
                prices = np.maximum.accumulate(prices)

                # Pass 2: enforce minimum growth per day to break plateaus.
                # Without this, isotonic pooling in BF smoothing causes e.g.
                # day-17 == day-24 price after rounding.
                # Rate 1.5%/day → 7-day gap yields ~11% minimum increase,
                # which always crosses one 10k rounding bucket for prices > 65k.
                MIN_GROWTH_RATE = 0.015
                for i in range(1, len(prices)):
                    day_gap = hari_vals[i] - hari_vals[i - 1]
                    min_price = prices[i - 1] * (1 + MIN_GROWTH_RATE * day_gap)
                    if prices[i] < min_price:
                        prices[i] = min_price

                # Pass 3: cap too-fast upward jumps (existing behaviour)
                for i in range(1, len(prices)):
                    max_allowed = prices[i - 1] * (1 + cap)
                    if prices[i] > max_allowed:
                        prices[i] = max_allowed

            prices = [p - (p % 10000) + 9000 for p in prices]
            group[col] = prices

        result.append(group)

    return pd.concat(result).reset_index(drop=True)

def enforce_bigdata_gap(df, sim_gap=50000, esim_gap=50000, flat_gap=25000):
    """
    Enforce minimum price gap untuk BIG DATA >= 20GB.
    Gap minimum per 10GB tambahan.
    """
    result = []

    bd_mask = (df["TYPE"] == "BIG DATA") & (df["KUOTA_NUM"] >= 20)
    bd = df[bd_mask].copy()
    non_bd = df[~bd_mask].copy()

    for (negara, hari), group in bd.groupby(["NEGARA", "HARI"]):
        group = group.sort_values("KUOTA_NUM").copy()
        prices_sim  = group["HARGA_SIM"].values.copy()
        prices_esim = group["HARGA_ESIM"].values.copy()
        prices_flat = group["HARGA_FLAT"].values.copy()
        gb_vals     = group["KUOTA_NUM"].values.copy()

        for i in range(1, len(gb_vals)):
            gb_diff   = gb_vals[i] - gb_vals[i - 1]
            ratio     = gb_diff / 10

            min_sim  = prices_sim[i - 1]  + ratio * sim_gap
            min_esim = prices_esim[i - 1] + ratio * esim_gap
            min_flat = prices_flat[i - 1] + ratio * flat_gap

            prices_sim[i]  = max(prices_sim[i],  min_sim)
            prices_esim[i] = max(prices_esim[i], min_esim)
            prices_flat[i] = max(prices_flat[i], min_flat)

        group["HARGA_SIM"]  = [p - (p % 10000) + 9000 for p in prices_sim]
        group["HARGA_ESIM"] = [p - (p % 10000) + 9000 for p in prices_esim]
        group["HARGA_FLAT"] = [p - (p % 10000) + 9000 for p in prices_flat]

        result.append(group)

    bd_fixed = pd.concat(result).reset_index(drop=True) if result else pd.DataFrame()
    return pd.concat([bd_fixed, non_bd]).reset_index(drop=True)

def compute_modal_adj(row):
    modal      = row["MODAL"]
    bs         = row["Behaviour_Score"]
    hari       = row["HARI"]
    quota_val  = row["QUOTA_VAL"]
    tipe       = row["TYPE"]

    if tipe == "BIG DATA":
        gb_discount       = row["GB_DISCOUNT"]
        duration_factor   = row["DURATION_FACTOR"]
        return modal * bs * quota_val * gb_discount * duration_factor

    elif tipe == "FUP":
        effective_days = hari * bs
        return modal * effective_days * quota_val 

    else:  # PURE UNLIMITED
        effective_days = hari * bs
        return modal * effective_days * quota_val


def export_pricing(bf_full, country_df):

    sheet = connect()

    # =====================
    # PREPARE DATA
    # =====================
    df = bf_full.copy()
    df = parse_sku(df)
    df["TYPE"] = df["KUOTA"].apply(get_type)
    df["KUOTA_ORIGINAL"] = df["KUOTA"]
    df["KUOTA_NUM"] = df["KUOTA"].apply(parse_quota_value)

    # =====================
    # WEIGHTED MODAL (OTOMATIS)
    # =====================
    package_modal = {}
    for package in country_df["Package"].unique():
        modal = calculate_weighted_modal(package, country_df)
        if modal is not None:
            package_modal[package] = modal

    temp = []
    EXLUDED_COUNTRIES = {"JPNMAX","CHMMAX"}
    for package, modal in package_modal.items():
        from processors.behaviour_factor import extract_country_code
        negara = extract_country_code(package)
        if negara != "UNK" and negara not in EXLUDED_COUNTRIES:
            temp.append({"NEGARA": negara, "MODAL_WEIGHTED": modal})

    if temp:
        modal_weighted_df = (
            pd.DataFrame(temp)
            .groupby("NEGARA")["MODAL_WEIGHTED"]
            .median()
            .reset_index()
        )
        df = df.merge(modal_weighted_df, on="NEGARA", how="left")
    else:
        df["MODAL_WEIGHTED"] = None

    modal_df = read_sheet(sheet, "MODAL_REFERENCE")[["NEGARA", "MODAL"]]
    df = df.merge(modal_df, on="NEGARA", how="left")

    df["MODAL_SOURCE"] = np.where(
        df["MODAL_WEIGHTED"].notna(), "WEIGHTED",
        np.where(df["MODAL"].notna(), "SHEET", "DEFAULT")
    )
    df["MODAL"] = np.where(
        df["MODAL_WEIGHTED"].notna(),
        df["MODAL_WEIGHTED"],
        pd.to_numeric(df["MODAL"], errors="coerce").fillna(3)
    )

    # =====================
    # DEBUG PRINT
    # =====================
    df["Behaviour_Score"] = pd.to_numeric(df["Behaviour_Score"], errors="coerce").fillna(0.4)
    print("\n📋 MODAL PER NEGARA:")
    modal_check = df.groupby("NEGARA")[["MODAL","MODAL_SOURCE"]].first().reset_index()
    print(modal_check.to_string(index=False))
    print(f"\nWeighted: {(modal_check['MODAL_SOURCE']=='WEIGHTED').sum()}")
    print(f"Sheet fallback: {(modal_check['MODAL_SOURCE']=='SHEET').sum()}")
    print(f"Default fallback: {(modal_check['MODAL_SOURCE']=='DEFAULT').sum()}\n")

    # =====================
    # COST (UPDATED LOGIC)
    # =====================
    df["QUOTA_VAL"] = df["KUOTA_NUM"].astype(float)

    df["GB_DISCOUNT"] = df["QUOTA_VAL"].apply(
        lambda gb: get_gb_discount(gb) if gb > 0 else 1.0
    )
    df["DURATION_FACTOR"] = df["HARI"].apply(get_duration_factor)

    df["MODAL_ADJ"] = df.apply(compute_modal_adj, axis=1)

    df["TOTAL_MODAL_SIM"] = df["MODAL_ADJ"] + DEVICE_SIM
    df["TOTAL_MODAL_ESIM"] = df["MODAL_ADJ"] + DEVICE_ESIM

    # =====================
    # CONVERT
    # =====================
    df["MODAL_ADJ_IDR"] = df["MODAL_ADJ"] * RATE 
    df["COST_SIM_IDR"] = df["TOTAL_MODAL_SIM"] * RATE
    df["COST_ESIM_IDR"] = df["TOTAL_MODAL_ESIM"] * RATE

    print(df[df["TYPE"].isin(["FUP","PURE UNLIMITED"])][
        ["SKU","MODAL","QUOTA_VAL","Behaviour_Score","HARI","MODAL_ADJ","MODAL_ADJ_IDR","COST_SIM_IDR"]
    ].sort_values("MODAL_ADJ_IDR", ascending=False).head(15).to_string())

    # =====================
    # PRICE (BEHAVIOUR-BASED — harga murni)
    # =====================
    df["HARGA_SIM"]  = df["COST_SIM_IDR"].apply(margin_sim)
    df["HARGA_ESIM"] = df["COST_ESIM_IDR"].apply(margin_esim)
    df["HARGA_FLAT"] = df["MODAL_ADJ_IDR"].apply(flat_price)


    # =====================
    # SORT (sebelum opportunity — supaya tier gap dihitung urut)
    # =====================
    TYPE_ORDER = {
        "BIG DATA": 1,
        "FUP": 2,
        "PURE UNLIMITED": 3
    }

    df["TYPE_SORT"] = df["TYPE"].map(TYPE_ORDER)
    df["KUOTA_SORT"] = df["KUOTA_NUM"].astype(float)

    df = df.sort_values(
        by=["TYPE_SORT", "HARI", "KUOTA_SORT"]
    ).drop(columns=["TYPE_SORT", "KUOTA_SORT"])

    # =====================
    # SMOOTH (behaviour-based prices)
    # =====================
    df = smooth_prices(df)
    df = enforce_bigdata_gap(df)
    # =====================
    # 🆕 COST OPPORTUNITY — after smooth & bigdata gap
    # =====================
    df = calculate_cost_opportunity(df)
    df = normalize_cross_type(df)
    df = apply_premium_pricing(df)
    df = smooth_prices(df)
    # Final safety: enforce minimum margin
    for col_price, col_cost in [
        ("HARGA_SIM", "COST_SIM_IDR"),
        ("HARGA_ESIM", "COST_ESIM_IDR"),
        ("HARGA_FLAT", "MODAL_ADJ_IDR"),
    ]:
        below = df[col_price] < df[col_cost]
        df.loc[below, col_price] = df.loc[below, col_cost].apply(
            lambda c: round_9000(c / (1 - TARGET_MARGIN))
        )
   
    # =====================
    # DEBUG: Print opportunity impact
    # =====================
    # Recalculate COST_OPP and MARGIN after premium adjustments
    df["COST_OPP_SIM"]  = (df["HARGA_SIM"]  - df["HARGA_BF_SIM"]).clip(lower=0)
    df["COST_OPP_ESIM"] = (df["HARGA_ESIM"] - df["HARGA_BF_ESIM"]).clip(lower=0)
    df["COST_OPP_FLAT"] = (df["HARGA_FLAT"] - df["HARGA_BF_FLAT"]).clip(lower=0)
    df["MARGIN_SIM"]  = ((df["HARGA_SIM"]  - df["COST_SIM_IDR"])  / df["HARGA_SIM"]  * 100).round(1)
    df["MARGIN_ESIM"] = ((df["HARGA_ESIM"] - df["COST_ESIM_IDR"]) / df["HARGA_ESIM"] * 100).round(1)
    df["MARGIN_FLAT"] = ((df["HARGA_FLAT"] - df["MODAL_ADJ_IDR"]) / df["HARGA_FLAT"] * 100).round(1)
    
    print("\n📊 COST OPPORTUNITY IMPACT:")
    for tipe in ["FUP", "BIG DATA", "PURE UNLIMITED"]:
        subset = df[df["TYPE"] == tipe]
        if subset.empty:
            continue
        avg_opp = subset["COST_OPP_FLAT"].mean()
        avg_margin = subset["MARGIN_FLAT"].mean()
        pct_lifted = (subset["COST_OPP_FLAT"] > 0).mean() * 100
        print(f"  {tipe}:")
        print(f"    Avg Cost Opportunity (FLAT): {avg_opp:,.0f} IDR")
        print(f"    Avg Margin (FLAT): {avg_margin:.1f}%")
        print(f"    SKUs lifted: {pct_lifted:.1f}%")
    
    # Premium comparison debug
    premium_countries = [n for n in df["NEGARA"].unique() if str(n).endswith("MAX")]
    if premium_countries:
        print("\n🏷️ PREMIUM PRICING COMPARISON:")
        for neg_max in premium_countries:
            neg_base = neg_max[:-3] + "PLUS"
            max_sub = df[df["NEGARA"] == neg_max]
            base_sub = df[df["NEGARA"] == neg_base]
            if base_sub.empty:
                continue
            merged = max_sub.merge(
                base_sub, on=["HARI", "KUOTA", "TYPE"], suffixes=("_MAX", "_BASE")
            )
            if merged.empty:
                continue
            merged["RATIO"] = merged["HARGA_FLAT_MAX"] / merged["HARGA_FLAT_BASE"]
            cheaper = (merged["HARGA_FLAT_MAX"] < merged["HARGA_FLAT_BASE"]).sum()
            same = (merged["HARGA_FLAT_MAX"] == merged["HARGA_FLAT_BASE"]).sum()
            print(f"  {neg_max} vs {neg_base}:")
            print(f"    Avg ratio: {merged['RATIO'].mean():.2f}×")
            print(f"    Min ratio: {merged['RATIO'].min():.2f}×")
            print(f"    MAX cheaper: {cheaper}, Same price: {same}")

    # =====================
    # FINAL COLUMN
    # =====================
    df["KUOTA"] = df["KUOTA_ORIGINAL"]
    df["MODAL"] = df["MODAL_ADJ"].round(2)

    final_cols = [
        "NEGARA",
        "SKU",
        "HARI",
        "KUOTA",
        "TYPE",
        "Behaviour_Score",
        "MODAL",
        # Harga Behaviour (murni dari BF)
        "HARGA_BF_FLAT",
        "HARGA_BF_SIM",
        "HARGA_BF_ESIM",
        # Cost Opportunity
        "COST_OPP_FLAT",
        "COST_OPP_SIM",
        "COST_OPP_ESIM",
        # Harga Final (setelah opportunity)
        "HARGA_FLAT",
        "HARGA_SIM",
        "HARGA_ESIM",
        # Margin
        "MARGIN_FLAT",
        "MARGIN_SIM",
        "MARGIN_ESIM",
        # Metadata
        "Source",
        "Confidence"
    ]

    df = df[final_cols]
    
    
    # =====================
    # UPLOAD
    # =====================
    try:
        ws = sheet.worksheet("PRICING_OUTPUT")
        ws.clear()
    except:
        ws = sheet.add_worksheet(title="PRICING_OUTPUT", rows="2000", cols="30")

    ws.update([df.columns.values.tolist()] + df.values.tolist())

    print("✅ PRICING EXPORTED")
    return df