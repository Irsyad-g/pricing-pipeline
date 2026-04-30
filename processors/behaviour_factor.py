import pandas as pd
import re
import json
import numpy as np
from pathlib import Path
from collections import Counter

CONFIG_PATH = Path("data/mappings/country_map.json")
LOG_PATH = Path("logs/unmapped_country.txt")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
MIN_SAMPLE_EXACT = 15

FUP_QUOTA_MAP = {
    "500": 0.5,
    "800": 0.8,
    "15":  1.5,
    "1":   1.0,
    "2":   2.0,
    "3":   3.0,
    "5":   5.0
}

with open(CONFIG_PATH, "r") as f:
    _raw = json.load(f)

# ── parse country_map v2 ────────────────────────────────────
# strip _meta, keep only country entries
COUNTRY_MAP = {k: v for k, v in _raw.items() if not k.startswith("_")}

# ── derived lookups (built once at import) ──────────────────
# group_name (uppercase) → region_group  (for split_country_dist)
# group_name (uppercase) → region_group  (replaces group_mapping.json + region_mapping.json)
_GROUP_TO_REGION_GROUP = {}
for _code, _data in COUNTRY_MAP.items():
    _g = _data.get("group", "").upper()
    if _g:
        _GROUP_TO_REGION_GROUP[_g] = _data.get("region_group", "OTHER")

# pattern (uppercase) → group_name  (replaces group_region.json)
_PATTERN_TO_GROUP = {}
for _code, _data in COUNTRY_MAP.items():
    _group = _data.get("group", "")
    for _p in _data.get("patterns", []):
        _PATTERN_TO_GROUP[_p.upper()] = _group


def get_group_lookup():
    """Return pattern→group dict for use in subscription_processor."""
    return dict(_PATTERN_TO_GROUP)


def get_group_to_region_group():
    """Return group→region_group dict for use in split_country_dist."""
    return dict(_GROUP_TO_REGION_GROUP)

# ────────────────────────────────────────────────────────────


def extract_country_raw(package_name):
    name = str(package_name).upper()
    match = re.split(r"\d+\s*DAYS?", name)
    if match:
        return match[0].strip()
    return name


def extract_days(package_name):
    match = re.search(r"(\d+)\s*Days?", str(package_name))
    if match:
        return int(match.group(1))
    return None


def extract_quota_value(package_name):
    name = str(package_name).upper()

    if "PURE UNLIMITED" in name:
        return "UNL"

    if "UNLIMITED" in name:
        match_gb = re.search(r"(\d+\.?\d*)\s*GB\/DAYS?", name)
        if match_gb:
            raw = match_gb.group(1)
            if raw == "1.5":
                return "15"
            return raw

        match_mb = re.search(r"(\d+)\s*MB\/DAYS?", name)
        if match_mb:
            return match_mb.group(1)

        return "UNL"

    match_big = re.search(r"(\d+)\s*GB", name)
    if match_big:
        return match_big.group(1) + "GB"

    match_fup = re.search(r"(\d+)\s*MB", name)
    if match_fup:
        return match_fup.group(1)

    return "UNK"


def extract_quota_type(package_name):
    """
    Return product category: "FUP", "BIG DATA", or "PURE UNLIMITED".
    """
    name = str(package_name).upper()

    if "UNLIMITED" in name:
        # FUP — pasti ada daily cap: GB/DAYS atau MB/DAYS
        if re.search(r"\d+\.?\d*\s*(GB|MB)\/DAYS?", name):
            return "FUP"
        # sisanya Pure Unlimited (apapun namanya)
        return "PURE UNLIMITED"

    # no "UNLIMITED" keyword → Big Data (fixed total GB)
    if re.search(r"\d+\s*GB", name):
        return "BIG DATA"

    return "UNK"


UNMAPPED = Counter()


def extract_country_code(package_name):
    name = str(package_name).upper()

    normalized = name.replace(" ", "")
    is_datamax = "DATAMAX" in normalized

    all_patterns = []

    for code, data in COUNTRY_MAP.items():
        if is_datamax and not code.endswith("MAX"):
            continue
        if not is_datamax and code.endswith("MAX"):
            continue

        for pattern in data["patterns"]:
            all_patterns.append((len(pattern), pattern.upper(), code))

    all_patterns.sort(reverse=True)

    for _, pattern, code in all_patterns:
        if pattern in name:
            return code

    raw_country = extract_country_raw(package_name)
    UNMAPPED[raw_country] += 1
    return "UNK"


def get_region(country_code):
    """Return granular region (ASIA_EAST, ASIA_SEA, etc) for BF fallback."""
    data = COUNTRY_MAP.get(country_code, {})
    return data.get("region", "OTHER")


def get_region_group(country_code):
    """Return coarse region (ASIA, EUROPE, etc) for report splitting."""
    data = COUNTRY_MAP.get(country_code, {})
    return data.get("region_group", "OTHER")


def resolve_group(package_name):
    """
    Resolve package name → group label.
    Uses pattern matching from country_map.
    Replaces the old _resolve_group + group_mapping.json system.
    """
    name = str(package_name).upper()

    # try longest pattern match first
    best_match = ""
    best_group = None

    for pattern, group in _PATTERN_TO_GROUP.items():
        if pattern in name and len(pattern) > len(best_match):
            best_match = pattern
            best_group = group

    if best_group:
        return best_group

    # fallback: strip digits, title-case
    m = re.match(r"^[^\d]+", name)
    raw = m.group(0).strip() if m else name.strip()
    return raw.title()


def build_sku(package_name):
    days = extract_days(package_name)
    quota = extract_quota_value(package_name)
    country = extract_country_code(package_name)
    return f"GK-{country}-{days}-{quota}"


def safe_split_sku(sku):
    if pd.isna(sku):
        return ["UNK", "UNK"]
    parts = str(sku).split("-")
    if len(parts) >= 4:
        return [parts[1], parts[3]]
    return ["UNK", "UNK"]


def generate_full_sku_matrix(final_df):

    ALL_QUOTA = [
        "500", "800", "15", "1", "2", "3", "5",
        "1GB", "2GB", "3GB", "5GB", "7GB", "8GB", "10GB", "15GB", "20GB", "30GB", "40GB", "50GB",
        "UNL"
    ]

    SPECIAL_DAY_RULES = {
        "HAJ": [7, 15, 30, 40, 60],
        "SAU": list(range(1,31)) + [40,60]
    }
    SPECIAL_QUOTA_RULES = {
        "JPNMAX": ["10"],
        "CHMMAX": ["10"]
    }

    rows = []

    base = final_df[["Package"]].drop_duplicates().copy()
    base["Country"] = base["Package"].apply(extract_country_code)
    countries_from_data = set(base[base["Country"] != "UNK"]["Country"].unique())
    forced_countries = {
        code for code, data in COUNTRY_MAP.items()
        if data.get("force_include", False)
    }

    countries = list(countries_from_data | forced_countries)

    for country in countries:
        if country in SPECIAL_DAY_RULES:
            day_list = SPECIAL_DAY_RULES[country]
        else:
            day_list = range(1, 31)
        quota_list = ALL_QUOTA.copy()

        if country in SPECIAL_QUOTA_RULES:
            quota_list += SPECIAL_QUOTA_RULES[country]

        for quota in quota_list:
            for day in day_list:
                rows.append({
                    "Country": country,
                    "Quota": quota,
                    "Duration_Days": day
                })
    full_df = pd.DataFrame(rows)
    return full_df


MAX_DAY_DIFF = 5


def get_confidence(source):
    return {
        "EXACT": 1.0,
        "REGION": 0.8,
        "GLOBAL": 0.6,
        "NEAREST": 0.4,
        "CURVE": 0.2
    }.get(source, 0.1)


def apply_curve(day, quota):
    quota = str(quota).upper()

    if "GB" in quota:
        return min(1.0, 0.6 + (10 / (day + 5)))

    if quota == "UNL":
        return max(0.3, 1 / np.sqrt(day))

    if quota in FUP_QUOTA_MAP:
        gb = FUP_QUOTA_MAP[quota]
        if gb < 1:
            return max(0.50, 0.90 - (day * 0.012))
        return max(0.40, 0.90 - (day * 0.02))

    return 0.5


def build_lookup_tables(known_df):
    known_df["Region"] = known_df["Country"].apply(get_region)

    exact_map = {}
    region_map = {}
    global_map = {}

    for _, row in known_df.iterrows():
        key_exact = (row["Country"], row["Duration_Days"], row["Quota"])
        key_global = (row["Duration_Days"], row["Quota"])

        exact_map[key_exact] = {
            "bf": row["Behaviour_Score"],
            "sample": row["Total_ICCID"]
        }
        country_data = COUNTRY_MAP.get(row["Country"], {})
        # "regions" for multi-region products, else fall back to single "region"
        regions = country_data.get("regions", [row["Region"]])

        for r in regions:
            key_region = (r, row["Duration_Days"], row["Quota"])
            region_map.setdefault(key_region, []).append(row["Behaviour_Score"])

        global_map.setdefault(key_global, []).append(row["Behaviour_Score"])

    region_map = {k: np.median(v) for k, v in region_map.items()}
    global_map = {k: np.median(v) for k, v in global_map.items()}

    return exact_map, region_map, global_map


def interpolate_behaviour(full_df, known_df):
    result = []

    exact_map, region_map, global_map = build_lookup_tables(known_df)

    quota_groups = {
        q: df.sort_values("Duration_Days")
        for q, df in known_df.groupby("Quota")
    }

    for _, row in full_df.iterrows():
        country = row["Country"]
        quota = row["Quota"]
        day = row["Duration_Days"]
        region = get_region(country)

        bf = None
        source = None

        # EXACT
        key = (country, day, quota)
        if key in exact_map and exact_map[key]["sample"] >= MIN_SAMPLE_EXACT:
            bf = exact_map[key]["bf"]
            source = "EXACT"

        # REGION
        elif (region, day, quota) in region_map:
            bf = region_map[(region, day, quota)]
            source = "REGION"

        # GLOBAL
        elif (day, quota) in global_map:
            bf = global_map[(day, quota)]
            source = "GLOBAL"

        # NEAREST
        elif quota in quota_groups:
            candidates = quota_groups[quota].copy()

            candidates_region = candidates[
                candidates["Country"].apply(get_region) == region
            ]

            if not candidates_region.empty:
                candidates = candidates_region

            candidates["diff"] = abs(candidates["Duration_Days"] - day)
            nearest = candidates.sort_values("diff").iloc[0]

            if nearest["diff"] <= MAX_DAY_DIFF:
                bf = nearest["Behaviour_Score"]
                source = "NEAREST"

        # CURVE
        if bf is None:
            bf = apply_curve(day, quota)
            source = "CURVE"

        result.append({
            "SKU": f"GK-{country}-{day}-{quota}",
            "Behaviour_Score": round(float(bf), 4),
            "Source": source,
            "Confidence": get_confidence(source),
            "Sample": exact_map[(country, day, quota)]["sample"] if source == "EXACT" else 0
        })

    return pd.DataFrame(result)


def quota_feasibility(quota, day, calibration=None):
    quota = str(quota).upper()

    if "GB" in quota:
        gb = float(quota.replace("GB", ""))
        gb_per_day = gb / day
        if calibration is not None:
            match = calibration[
                (calibration["Quota_Type"] == "BIG DATA") &
                (calibration["Quota"] == quota)
            ]
            DAILY_REFERENCE = match["Daily_Ref"].values[0] if not match.empty else 1.35
        else:
            DAILY_REFERENCE = 1.35
        qff = (DAILY_REFERENCE / gb_per_day) * 0.8
        max_cap = max(0.50, 0.85 - (gb * 0.008))
        return max(0.25, min(max_cap, qff))

    elif quota in FUP_QUOTA_MAP:
        gb = FUP_QUOTA_MAP[quota]
        if gb <= 0.5:
            base = 0.55
        elif gb <= 0.8:
            base = 0.52
        elif gb <= 1.0:
            base = 0.50
        elif gb <= 1.5:
            base = 0.48
        elif gb <= 2.0:
            base = 0.46
        elif gb <= 3.0:
            base = 0.44
        else:
            base = 0.40

        if calibration is not None:
            match = calibration[
                (calibration["Quota_Type"] == "FUP") &
                (calibration["Quota"] == quota)
            ]
            if not match.empty:
                activity_rate = match["Activity_Rate"].values[0]
                base = base * activity_rate

        decay = max(0.20, base - (day * 0.008))
        return decay

    else:  # UNL
        base = 0.75
        if calibration is not None:
            match = calibration[calibration["Quota_Type"] == "PURE UNLIMITED"]
            if not match.empty:
                activity_rate = match["Activity_Rate"].values[0]
                base = base * activity_rate
        decay = max(0.35, base - (day * 0.012))
        return decay


def clamp_behaviour_scores(bf_full, calibration=None):
    def clamp(row):
        score = row["Behaviour_Score"]
        quota = str(row["Quota"]).upper()
        day = row["Duration_Days"]
        source = row["Source"]
        sample = row.get("Sample", 0)

        qff = quota_feasibility(quota, day, calibration)

        if "GB" in quota:
            if source == "EXACT":
                data_weight = min(0.80, 0.55 + (sample / 500) * 0.25)
            elif source == "REGION":
                data_weight = 0.40
            elif source == "GLOBAL":
                data_weight = 0.25
            elif source == "NEAREST":
                data_weight = 0.15
            else:
                data_weight = 0.05
        else:
            if source == "EXACT":
                data_weight = min(0.92, 0.65 + (sample / 500) * 0.27)
            elif source == "REGION":
                data_weight = 0.60
            elif source == "GLOBAL":
                data_weight = 0.45
            elif source == "NEAREST":
                data_weight = 0.30
            else:
                data_weight = 0.10

        qff_weight = 1 - data_weight
        blended = (score * data_weight) + (qff * qff_weight)
        return round(blended, 4)

    bf_full = bf_full.copy()
    bf_full["Behaviour_Score"] = bf_full.apply(clamp, axis=1)
    return bf_full


def smooth_behaviour_scores(bf_full):
    from sklearn.isotonic import IsotonicRegression
    result = []

    for (country, quota), group in bf_full.groupby(["Country", "Quota"]):
        group = group.sort_values("Duration_Days").copy()
        days = group["Duration_Days"].values
        scores = group["Behaviour_Score"].values

        if len(scores) > 1:
            quota_upper = str(quota).upper()

            if "GB" in quota_upper:
                ir = IsotonicRegression(increasing=True, out_of_bounds="clip")
            else:
                ir = IsotonicRegression(increasing=False, out_of_bounds="clip")

            smoothed = ir.fit_transform(days, scores)
        else:
            smoothed = scores

        group["Behaviour_Score"] = smoothed
        result.append(group)

    return pd.concat(result).reset_index(drop=True)


def parse_sku_parts(sku):
    parts = str(sku).split("-")
    if len(parts) >= 4:
        return pd.Series({"Country": parts[1], "Quota": parts[3]})
    return pd.Series({"Country": "UNK", "Quota": "UNK"})


def build_qff_calibration(df):
    df = df.copy()
    df["GB_Per_Active_Day"] = (
        df["Total Usage (MB)"] / 1024 /
        df["Actual Days"].clip(lower=1)
    )
    df["Activity_Rate"] = (
        df["Actual Days"] / df["Duration_Days"].clip(lower=1)
    ).clip(upper=1)

    calibration = (
        df.groupby(["Quota_Type", "Quota"])
        .agg(
            Daily_Ref=("GB_Per_Active_Day", "median"),
            Activity_Rate=("Activity_Rate", "median"),
            Sample=("ICCID", "nunique")
        )
        .reset_index()
    )

    return calibration


def calculate_behaviour_factor(df):

    df = df[df["Total Quota (MB)"] > 0].copy()

    df["Duration_Days"] = df["Package"].apply(extract_days)
    df["Quota_Type"] = df["Package"].apply(extract_quota_type)
    df["Quota"] = df["Package"].apply(extract_quota_value)
    calibration = build_qff_calibration(df)

    package_level = (
        df.groupby("Package")
        .agg(
            Total_ICCID=("ICCID", "nunique"),
            Total_Quota_MB=("Total Quota (MB)", "first"),
            Duration_Days=("Duration_Days", "first"),
            Quota_Type=("Quota_Type", "first"),
            P50_Usage_Ratio=("Usage Ratio", "median"),
            P75_Usage_Ratio=("Usage Ratio", lambda x: x.quantile(0.75))
        )
        .reset_index()
    )
    
    package_level["Quota"] = package_level["Package"].apply(extract_quota_value)

    group_level = (
        df.groupby(["Duration_Days", "Quota"])
        .agg(
            Group_P50=("Usage Ratio", "median"),
            Group_P75=("Usage Ratio", lambda x: x.quantile(0.75))
        )
        .reset_index()
    )

    bf = package_level.merge(
        group_level,
        on=["Duration_Days", "Quota"],
        how="left"
    )
    MIN_SAMPLE = 25
    bf["Weight"] = (bf["Total_ICCID"] / MIN_SAMPLE).clip(upper=1)
    bf["Final_P50"] = (
        bf["Weight"] * bf["P50_Usage_Ratio"] +
        (1 - bf["Weight"]) * bf["Group_P50"]
    )
    bf["Final_P75"] = (
        bf["Weight"] * bf["P75_Usage_Ratio"] +
        (1 - bf["Weight"]) * bf["Group_P75"]
    )
    bf["Behaviour_Score"] = (
        0.6 * bf["Final_P50"] +
        0.4 * bf["Final_P75"]
    )

    target_matrix = (
        bf.groupby(["Duration_Days", "Quota"])
        .agg(Target_BF=("Behaviour_Score", "median"))
        .reset_index()
    )
    bf = bf.merge(
        target_matrix,
        on=["Duration_Days", "Quota"],
        how="left"
    )
    bf["SKU"] = bf["Package"].apply(build_sku)
    bf["Country"] = bf["Package"].apply(extract_country_code)
    bf["Quota"] = bf["Package"].apply(extract_quota_value)
    bf = bf[~bf["SKU"].str.contains("UNK")]
    bf = bf[
        (bf["Country"] != "UNK") &
        (bf["Quota"] != "UNK")
    ]
    if UNMAPPED:
        with open(LOG_PATH, "w") as f:
            for country, count in UNMAPPED.items():
                f.write(f"{country} | {count}\n")

    full_df = generate_full_sku_matrix(df)
    known_df = bf.copy()
    bf_full = interpolate_behaviour(full_df, known_df)

    bf_full[["Country", "Quota"]] = bf_full["SKU"].apply(parse_sku_parts)
    bf_full["Duration_Days"] = bf_full["SKU"].apply(
        lambda s: int(s.split("-")[2]) if len(s.split("-")) >= 4 else 0
    )
    bf_full = clamp_behaviour_scores(bf_full, calibration)
    bf_full = smooth_behaviour_scores(bf_full)
    return bf, bf_full