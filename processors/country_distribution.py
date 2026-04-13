import pandas as pd

from processors.behaviour_factor import get_group_to_region_group


def drop_zero_country_columns(df):
    keep_cols = ["Group", "total_iccid"]

    country_cols = [
        c for c in df.columns
        if c not in keep_cols
    ]

    non_zero_cols = [
        c for c in country_cols
        if df[c].sum() != 0
    ]

    return df[keep_cols + non_zero_cols]


def build_country_distribution(country_df):
    base = (
        country_df[["Group", "Country", "ICCID"]]
        .drop_duplicates()
    )

    total_iccid = (
        base.groupby("Group")["ICCID"]
        .nunique()
        .reset_index(name="total_iccid")
    )

    appearance = (
        base.groupby(["Group", "Country"])["ICCID"]
        .nunique()
        .reset_index(name="COUNT")
    )

    appearance["Country"] = (
        appearance["Country"]
        .str.upper()
        .str.replace(r"[^\w]+", "_", regex=True)
    )

    wide = appearance.pivot(
        index="Group",
        columns="Country",
        values="COUNT"
    ).fillna(0).reset_index()

    count_cols = [c for c in wide.columns if c != "Group"]
    wide.rename(columns={c: f"{c}_COUNT" for c in count_cols}, inplace=True)

    wide = wide.merge(total_iccid, on="Group", how="left")

    for col in count_cols:
        wide[f"{col}_RATIO"] = wide[f"{col}_COUNT"] / wide["total_iccid"]

    fixed = ["Group", "total_iccid"]
    others = sorted(c for c in wide.columns if c not in fixed)

    return wide[fixed + others]


def split_country_dist_by_region(df, region):
    """
    Filter country distribution by coarse region.

    Changed from old signature: split_country_dist_by_region(df, region, region_json, group_region)
    Now uses get_group_to_region_group() from country_map directly.
    No more broken case-sensitivity issues.
    """
    group_to_rg = get_group_to_region_group()

    filtered = df[
        df["Group"].apply(
            lambda g: group_to_rg.get(g.upper(), "OTHER") == region.upper()
        )
    ]

    filtered = drop_zero_country_columns(filtered)

    return filtered