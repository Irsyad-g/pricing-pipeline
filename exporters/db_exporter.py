import pandas as pd
from sqlalchemy import text
from config.database import get_engine

def _upsert_final(df, engine):
    if df.empty:
        print("processed.final_output — kosong, skip")
        return

    with engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT iccid, start_date, package, total_usage_mb, total_quota_mb FROM processed.final_output",
            conn
        )

    existing["start_date"] = pd.to_datetime(existing["start_date"], errors="coerce")
    df["start_date"]       = pd.to_datetime(df["start_date"], errors="coerce")
    df["iccid"]            = df["iccid"].astype(str)
    existing["iccid"]      = existing["iccid"].astype(str)

    # baru → insert
    merged   = df.merge(existing[["iccid", "start_date"]], on=["iccid", "start_date"], how="left", indicator=True)
    new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    # sudah ada tapi perlu update:
    #   - usage masih 0/null
    #   - package berisi UNK (country code belum ter-resolve)
    existing_needs_update = existing[
        (existing["total_usage_mb"].isna() | (existing["total_usage_mb"] == 0) |
         existing["total_quota_mb"].isna() | (existing["total_quota_mb"] == 0) |
         existing["package"].str.contains("UNK", case=False, na=False))
    ][["iccid", "start_date"]]
    update_rows = df.merge(existing_needs_update, on=["iccid", "start_date"], how="inner")

    if not new_rows.empty:
        new_rows.to_sql("final_output", engine, schema="processed", if_exists="append", index=False)
        print(f"  processed.final_output — insert {len(new_rows)} baris baru")

    if not update_rows.empty:
        with engine.begin() as conn:
            for _, row in update_rows.iterrows():
                conn.execute(text("""
                    UPDATE processed.final_output
                    SET package        = :package,
                        total_usage_mb = :usage,
                        total_quota_mb = :quota,
                        real_cost_cny  = :cost_cny,
                        real_cost_idr  = :cost_idr,
                        usage_ratio    = :ratio
                    WHERE iccid = :iccid AND start_date = :start_date
                """), {
                    "package":    row.get("package", ""),
                    "usage":      row.get("total_usage_mb"),
                    "quota":      row.get("total_quota_mb"),
                    "cost_cny":   row.get("real_cost_cny"),
                    "cost_idr":   row.get("real_cost_idr"),
                    "ratio":      row.get("usage_ratio"),
                    "iccid":      row["iccid"],
                    "start_date": row["start_date"],
                })
        # breakdown alasan update
        unk_count = existing_needs_update.merge(
            existing[existing["package"].str.contains("UNK", case=False, na=False)][["iccid", "start_date"]],
            on=["iccid", "start_date"], how="inner"
        )
        usage_zero = len(update_rows) - len(unk_count)
        print(f"  processed.final_output — update {len(update_rows)} baris "
              f"(usage=0/null: {usage_zero}, package=UNK: {len(unk_count)})")

    if new_rows.empty and update_rows.empty:
        print("  processed.final_output — semua data sudah up to date")


def _insert(df, table, schema, engine, unique_cols=None):
    if df.empty:
        print(f"  {schema}.{table} — kosong, skip")
        return

    if unique_cols:
        with engine.connect() as conn:
            existing = pd.read_sql(
                f"SELECT {', '.join(unique_cols)} FROM {schema}.{table}",
                conn
            )
        for col in unique_cols:
            if col in df.columns and col in existing.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    existing[col] = pd.to_datetime(existing[col], errors="coerce")
                else:
                    existing[col] = existing[col].astype(str)
                    df[col] = df[col].astype(str)

        before = len(df)
        df = df.merge(existing, on=unique_cols, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
        skipped = before - len(df)
        if skipped > 0:
            print(f"  {schema}.{table} — skip {skipped} baris duplikat")

    if df.empty:
        print(f"  {schema}.{table} — semua data sudah ada")
        return

    df.to_sql(table, engine, schema=schema, if_exists="append", index=False)
    print(f"  {schema}.{table} — insert {len(df)} baris")


def export_to_db(daily, sub, final, bf_full, pricing_df, daily_files=None, sub_files=None):
    engine = get_engine()

    daily_db = daily[["ICCID", "AREA", "DATE", "DATE_ONLY", "USAGE_MB"]].copy()
    daily_db.columns = ["iccid", "area", "date", "date_only", "usage_mb"]
    daily_db["source_file"] = ", ".join([f.name for f in daily_files]) if daily_files else ""
    _insert(daily_db, "daily_usage", "raw", engine,
            unique_cols=["iccid", "date", "area"])

    sub_db = sub[["ICCID", "PACKAGE", "STATUS", "START", "END", "DAYS"]].copy()
    sub_db.columns = ["iccid", "package", "status", "start_date", "end_date", "days"]
    sub_db["source_file"] = ", ".join([f.name for f in sub_files]) if sub_files else ""
    _insert(sub_db, "subscription", "raw", engine,
            unique_cols=["iccid", "start_date", "status"])

    final_db = final.copy()
    final_db = final_db.rename(columns={
        "Type":             "type",
        "Group":            "grp",
        "Package":          "package",
        "ICCID":            "iccid",
        "Month":            "month",
        "Start":            "start_date",
        "End":              "end_date",
        "Total Quota (MB)": "total_quota_mb",
        "Total Usage (MB)": "total_usage_mb",
        "Usage Ratio":      "usage_ratio",
        "Actual Days":      "actual_days",
        "Visit Area":       "visit_area",
        "Real_Cost_CNY":    "real_cost_cny",
        "Real_Cost_IDR":    "real_cost_idr",
    })
    _upsert_final(final_db, engine)

    bf_db = bf_full[["SKU", "Behaviour_Score", "Source", "Confidence", "Sample"]].copy()
    bf_db.columns = ["sku", "behaviour_score", "source", "confidence", "sample"]
    bf_db.to_sql("behaviour_factor", engine, schema="processed",
                 if_exists="replace", index=False)
    print(f"  processed.behaviour_factor — replace {len(bf_db)} baris")

    pricing_db = pricing_df.rename(columns={
        "NEGARA":          "negara",
        "SKU":             "sku",
        "HARI":            "hari",
        "KUOTA":           "kuota",
        "TYPE":            "type",
        "Behaviour_Score": "behaviour_score",
        "MODAL":           "modal",
        "HARGA_FLAT":      "harga_flat",
        "HARGA_SIM":       "harga_sim",
        "HARGA_ESIM":      "harga_esim",
        "Source":          "source",
        "Confidence":      "confidence",
    })
    pricing_db.to_sql("pricing_output", engine, schema="pricing",
                      if_exists="replace", index=False)
    print(f"  pricing.pricing_output — replace {len(pricing_db)} baris")

    print("\n  DB EXPORT DONE")