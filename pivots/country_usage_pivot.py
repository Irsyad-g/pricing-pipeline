import pandas as pd

def build_country_usage_pivot(country_df):
    df = country_df.copy()

    # Normalisasi nama negara → aman jadi kolom
    df["Country"] = (
        df["Country"]
        .str.upper()
        .str.replace(r"[^\w]+", "_", regex=True)
    )

    usage_pivot = df.pivot_table(
        index=["Group", "ICCID", "Package", "Total Quota (MB)"],
        columns="Country",
        values="Country Usage (MB)",
        aggfunc="sum",
        fill_value=0
    )

    # Flatten columns
    usage_pivot.columns = [f"{c}_MB" for c in usage_pivot.columns]
    usage_pivot = usage_pivot.reset_index()

    # Add ratio columns
    for col in usage_pivot.columns:
        if col.endswith("_MB") and col != "Total Quota (MB)":
            country = col.replace("_MB", "")
            usage_pivot[f"{country}_RATIO"] = (
                usage_pivot[col] / usage_pivot["Total Quota (MB)"]
            )

    return usage_pivot