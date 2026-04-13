"""
Script sekali jalan untuk mengisi real_cost_cny dan real_cost_idr
di processed.final_output yang sudah ada di DB.
"""
import pandas as pd
from sqlalchemy import text
from config.database import get_engine
from processors.cost_calculator import calculate_cost_per_subscription
from loaders import load_daily_usage, load_subscription
from rules.quota_rules import extract_quota
from processors import process_subscription
from config import RAW_DATA, GROUP_MAP

if __name__ == "__main__":
    print("Loading data...")
    sub_files   = list(RAW_DATA.glob("SUBSCRIPTION_*.xlsx"))
    daily_files = (
        list(RAW_DATA.glob("DAILY_USAGE_*.xlsx")) +
        list(RAW_DATA.glob("BSN-*.xlsx"))
    )

    daily = load_daily_usage(daily_files)
    sub   = load_subscription(sub_files)
    sub["TOTAL_QUOTA_MB"] = sub.apply(
        lambda x: extract_quota(x["PACKAGE"], x["DAYS"]), axis=1
    )
    final, _ = process_subscription(sub, daily, GROUP_MAP)

    print("Menghitung cost per subscription...")
    cost = calculate_cost_per_subscription(final, daily)
    print(f"  Cost dihitung: {len(cost)} subscription")

    print("Update DB...")
    engine = get_engine()
    updated = 0
    with engine.begin() as conn:
        for _, row in cost.iterrows():
            result = conn.execute(text("""
                UPDATE processed.final_output
                SET real_cost_cny = :cny,
                    real_cost_idr = :idr
                WHERE iccid = :iccid
                AND start_date = :start
            """), {
                "cny":   float(row["Real_Cost_CNY"]),
                "idr":   float(row["Real_Cost_IDR"]),
                "iccid": str(row["ICCID"]),
                "start": row["Start"],
            })
            updated += result.rowcount

    print(f"  Updated: {updated} baris")

    # verifikasi
    with engine.connect() as conn:
        result = pd.read_sql("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN real_cost_idr > 0 THEN 1 END) as terisi,
                COUNT(CASE WHEN real_cost_idr = 0 THEN 1 END) as kosong
            FROM processed.final_output
        """, conn)
    print("\nVerifikasi:")
    print(result.to_string(index=False))
    print("\nDone!")