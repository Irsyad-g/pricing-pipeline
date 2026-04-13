import time

from config import (
    RAW_DATA,
    OUTPUT_FILE,
)

from loaders import load_daily_usage, load_subscription
from rules.quota_rules import extract_quota
from processors import (
    process_subscription,
    build_country_distribution,
    split_country_dist_by_region
)
from pivots import build_country_usage_pivot
from summaries.month_summary import build_month_summary
from summaries.summary import build_summary, build_base_factor
from processors.behaviour_factor import calculate_behaviour_factor
from exporters.excel_exporter import export_all
from exporters.google_sheets_exporter import export_pricing
from exporters.db_exporter import export_to_db

# ── helper ──────────────────────────────────────────────────
_t = {}

def t_start(label):
    _t[label] = time.perf_counter()

def t_end(label):
    elapsed = time.perf_counter() - _t.pop(label)
    print(f"  ⏱  {label:<45s} {elapsed:.2f}s")

def t_report_total(start):
    print(f"\n  ⏱  {'TOTAL':<45s} {time.perf_counter() - start:.2f}s")
# ────────────────────────────────────────────────────────────


def main():
    total_start = time.perf_counter()
    print("\n🚀 PIPELINE START\n")

    # 1. LOAD
    t_start("load")
    sub_files   = list(RAW_DATA.glob("SUBSCRIPTION_*.xlsx"))
    daily_files = (
        list(RAW_DATA.glob("DAILY_USAGE_*.xlsx")) +
        list(RAW_DATA.glob("BSN-*.xlsx"))
    )
    daily = load_daily_usage(daily_files)
    sub   = load_subscription(sub_files)
    t_end("load")

    # 2. EXTRACT QUOTA
    t_start("extract_quota")
    sub["TOTAL_QUOTA_MB"] = sub.apply(
        lambda x: extract_quota(x["PACKAGE"], x["DAYS"]), axis=1
    )
    t_end("extract_quota")

    # 3. PROCESS SUBSCRIPTION — no more group_map parameter
    t_start("process_subscription")
    final, country_df = process_subscription(sub, daily)
    t_end("process_subscription")

    # 3b. CALCULATE COST
    t_start("calculate_cost")
    from processors.cost_calculator import calculate_cost_per_subscription
    cost  = calculate_cost_per_subscription(final, daily)
    final = final.merge(cost, on=["ICCID", "Start"], how="left")
    final["Real_Cost_CNY"] = final["Real_Cost_CNY"].fillna(0)
    final["Real_Cost_IDR"] = final["Real_Cost_IDR"].fillna(0)
    t_end("calculate_cost")

    # 4. PIVOTS & DISTRIBUTIONS
    t_start("build_country_usage_pivot")
    usage_pivot = build_country_usage_pivot(country_df)
    t_end("build_country_usage_pivot")

    t_start("build_country_distribution")
    country_dist_wide = build_country_distribution(country_df)
    t_end("build_country_distribution")

    # 5. BEHAVIOUR FACTOR
    t_start("calculate_behaviour_factor")
    bf_table, bf_full = calculate_behaviour_factor(final)
    t_end("calculate_behaviour_factor")

    # 6. SPLIT REGION — no more region_json / group_region parameters
    t_start("split_country_dist (all regions)")
    country_dist_asia        = split_country_dist_by_region(country_dist_wide, "ASIA")
    country_dist_europe      = split_country_dist_by_region(country_dist_wide, "EUROPE")
    country_dist_middle_east = split_country_dist_by_region(country_dist_wide, "MIDDLE_EAST")
    country_dist_america     = split_country_dist_by_region(country_dist_wide, "AMERICA")
    country_dist_oceania     = split_country_dist_by_region(country_dist_wide, "OCEANIA")
    t_end("split_country_dist (all regions)")

    # 7. SUMMARIES
    t_start("build_summary + month_summary + base")
    summary       = build_summary(final)
    month_summary = build_month_summary(final)
    base          = build_base_factor(final)
    t_end("build_summary + month_summary + base")

    # 8. EXPORT PRICING → Google Sheets
    t_start("export_pricing (Google Sheets)")
    pricing_df = export_pricing(bf_full, country_df)
    t_end("export_pricing (Google Sheets)")

    # 9. EXPORT DB
    t_start("export_to_db")
    export_to_db(
        daily=daily,
        sub=sub,
        final=final,
        bf_full=bf_full,
        pricing_df=pricing_df,
        daily_files=daily_files,
        sub_files=sub_files
    )
    t_end("export_to_db")

    # 10. EXPORT EXCEL
    t_start("export_all (Excel)")
    export_all(
        OUTPUT_FILE,
        ALL_DATA=final,
        SUMMARY=summary,
        MONTH_SUMMARY=month_summary,
        Behaviour_Factor=bf_table,
        Behaviour_Full=bf_full,
        Base_Factor=base
    )
    t_end("export_all (Excel)")

    t_report_total(total_start)
    print("\n✅ PIPELINE DONE\n")


if __name__ == "__main__":
    main()