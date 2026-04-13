import pandas as pd

def build_summary(final: pd.DataFrame) -> pd.DataFrame:
    thresholds = {
        "BIG DATA": 0.9,
        "FUP": 0.6,
        "PURE UNLIMITED": 0.6
    }

    summary = final.groupby(["Type", "Group"]).agg(
        qty_iccid=("ICCID", "nunique"),
        avg_ratio=("Usage Ratio", "mean"),
        min_ratio=("Usage Ratio", "min"),
        median_ratio=("Usage Ratio", "median"),
        max_ratio=("Usage Ratio", "max"),
    ).reset_index()

    summary["Threshold"] = summary["Type"].map(thresholds)

    summary["qty_below"] = summary.apply(
        lambda r: (
            final[
                (final["Type"] == r["Type"]) &
                (final["Group"] == r["Group"]) &
                (final["Usage Ratio"] <= r["Threshold"])
            ].shape[0]
        ),
        axis=1
    )

    summary["qty_above"] = summary["qty_iccid"] - summary["qty_below"]

    return summary

def build_base_factor(final: pd.DataFrame) -> pd.DataFrame:
    base = (
        final.groupby("Type")
        .agg(
            total_iccid=("ICCID", "nunique"),
            weighted_avg_ratio=("Usage Ratio", "mean")
        )
        .reset_index()
    )

    return base