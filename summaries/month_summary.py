import pandas as pd

def build_month_summary(final: pd.DataFrame) -> pd.DataFrame:
    return final.groupby(["Month", "Type"]).agg(
        qty_iccid=("ICCID", "nunique"),
        avg_ratio=("Usage Ratio", "mean"),
        min_ratio=("Usage Ratio", "min"),
        median_ratio=("Usage Ratio", "median"),
        max_ratio=("Usage Ratio", "max"),
    ).reset_index()