import pandas as pd
import numpy as np
import json
import re
import argparse
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
from config.database import get_engine
from config.commission import get_commission
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

RATE              = 2450
RAW_DIR           = Path("data/raw")
MCC_MAP_PATH      = Path("data/mappings/mcc_map.json")
COUNTRY_RATE_PATH = Path("data/mappings/country_rate.json")
OUTPUT_DIR        = Path("data/output")
MANUAL_INPUT_PATH = OUTPUT_DIR / "manual_input.xlsx"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

with open(MCC_MAP_PATH)      as f: MCC_MAP      = json.load(f)
with open(COUNTRY_RATE_PATH) as f: COUNTRY_RATE = json.load(f)

GLOBAL_RATE = np.median(list(COUNTRY_RATE.values()))

BACKLOG_COLS = ["Channel", "Invoice", "ICCID", "SKU", "Product_Name",
                "Product_Type", "Order_Date", "Month", "Harga_Jual"]

# ── SKU normalization (single place, reusable) ────────────────
SKU_PREFIX_RULES = [
    (r"^TU-GK-",   "GK-"),
    (r"^GM-GK-",   "GK-"),
    (r"^MM-GK-",   "GK-"),
]

SKU_MARKETPLACE_RULES = SKU_PREFIX_RULES + [
    (r"^GK-SEA4-",  "GK-SEA5-"),
    (r"^GK-SEA6-",  "GK-SEA5-"),
    (r"^GK-JPN-",   "GK-JPNPLUS-"),
    (r"^WG-SAU-",   "GK-SAU-"),
    (r"^WG-SGMY-",  "GK-SGMY-"),
    (r"^WG-JPN-",   "GK-JPNPLUS-"),
    (r"^WG-JPNM-",  "GK-JPNPLUS-"),
    (r"^WG-SEA5-",  "GK-SEA5-"),
    (r"^SM-USA-",   "GK-USA-"),
    (r"^GM-",       "GK-"),
]

VALID_SKU_PREFIXES = ("GK-", "GM-GK-", "TU-GK", "MM-GK", "WG-", "SM-")


def normalize_sku(sku, rules=None):
    """Apply SKU normalization rules."""
    if not sku or not isinstance(sku, str):
        return sku
    s = sku.strip().upper()
    for pattern, replacement in (rules or SKU_PREFIX_RULES):
        s = re.sub(pattern, replacement, s)
    return s


def extract_mcc(country_str):
    m = re.match(r"(\d+)", str(country_str).strip())
    return m.group(1) if m else None


def get_rate(country_str):
    mcc          = extract_mcc(country_str)
    country_name = MCC_MAP.get(str(mcc)) if mcc else None
    rate         = COUNTRY_RATE.get(country_name) if country_name else None
    return rate if rate else GLOBAL_RATE


def parse_args():
    parser = argparse.ArgumentParser(description="Generate margin report")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date filter (YYYY-MM-DD). Filter output only.")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date filter (YYYY-MM-DD). Filter output only.")
    parser.add_argument("--output", type=str, default=None,
                        help="Custom output filename (default: margin_report.xlsx or with date range)")
    return parser.parse_args()


def get_output_path(args):
    if args.output:
        return OUTPUT_DIR / args.output
    if args.start_date and args.end_date:
        return OUTPUT_DIR / f"margin_report_{args.start_date}_to_{args.end_date}.xlsx"
    if args.start_date:
        return OUTPUT_DIR / f"margin_report_from_{args.start_date}.xlsx"
    if args.end_date:
        return OUTPUT_DIR / f"margin_report_until_{args.end_date}.xlsx"
    return OUTPUT_DIR / "margin_report.xlsx"


def load_orders():
    files = list(RAW_DIR.glob("orders_export*.xlsx"))
    if not files:
        raise FileNotFoundError(f"Tidak ada file orders_export*.xlsx di {RAW_DIR}")

    print(f"  Ditemukan {len(files)} file orders_export:")
    dfs = []
    for f in files:
        print(f"  - {f.name}")
        df = pd.read_excel(f, header=0, dtype=str)
        temp = pd.DataFrame({
            "Channel":      df.iloc[:, 0],
            "Status":       df.iloc[:, 2],
            "Invoice":      df.iloc[:, 3],
            "Order_Date":   df.iloc[:, 7],
            "Product_Name": df.iloc[:, 14],
            "SKU_Original": df.iloc[:, 13],
            "Product_Type": df.iloc[:, 11],
            "Harga_Jual":   df.iloc[:, 17],
            "ICCID":        df.iloc[:, 19],
        })
        dfs.append(temp)

    orders = pd.concat(dfs, ignore_index=True)
    orders["Status_clean"] = orders["Status"].astype(str).str.strip().str.upper()
    orders["status_priority"] = np.where(orders["Status_clean"] == "DONE", 1, 2)
    orders = (
        orders
        .sort_values(["ICCID", "status_priority", "Order_Date"],
                      ascending=[True, True, False])
        .drop_duplicates(subset=["ICCID"], keep="first")
    )
    orders["Order_Date"]   = pd.to_datetime(orders["Order_Date"], errors="coerce")
    orders["Month"]        = orders["Order_Date"].dt.strftime("%Y-%b")
    orders["Channel"]      = orders["Channel"].str.strip()
    orders["Invoice"]      = orders["Invoice"].str.strip().str.lstrip("'")
    orders["Product_Name"] = orders["Product_Name"].str.strip()
    orders["SKU_Original"] = orders["SKU_Original"].str.strip().str.upper()
    orders["Product_Type"] = orders["Product_Type"].str.strip()
    orders["ICCID"]        = orders["ICCID"].str.strip().str.lstrip("'")
    orders["Harga_Jual"]   = pd.to_numeric(orders["Harga_Jual"], errors="coerce").fillna(0)

    before = len(orders)
    orders = orders.drop_duplicates()
    print(f"  Duplikat dihapus: {before - len(orders)} baris")

    # filter ICCID valid
    orders = orders[orders["ICCID"].str.startswith("898", na=False)]
    orders = orders[orders["SKU_Original"].notna() & (orders["SKU_Original"] != "")]

    # expand SKU prefix filter — include WG- dan SM- yang akan di-normalize
    orders = orders[orders["SKU_Original"].str.startswith(VALID_SKU_PREFIXES, na=False)]

    orders["Product_Type"] = np.where(
        orders["SKU_Original"].str.startswith("GM-GK-", na=False),
        "Simcard", orders["Product_Type"]
    )

    # normalize SKU (single place)
    orders["SKU"] = orders["SKU_Original"].apply(
        lambda s: normalize_sku(s, SKU_PREFIX_RULES)
    )

    return orders


def load_legacy_orders():
    legacy_path = RAW_DIR / "orders_legacy.xlsx"
    if not legacy_path.exists():
        print("  Tidak ada file legacy orders")
        return pd.DataFrame(columns=["Invoice", "ICCID", "SKU", "Order_Date"])

    df = pd.read_excel(legacy_path, dtype=str)
    df["Invoice"]    = df["Invoice"].str.strip().str.lstrip("'")
    df["ICCID"]      = df["ICCID"].str.strip().str.lstrip("'")
    df["SKU"]        = df["SKU"].str.strip().str.upper().apply(
        lambda s: normalize_sku(s, SKU_PREFIX_RULES)
    )
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")
    df["Month"]      = df["Order_Date"].dt.strftime("%Y-%b")
    df = df.drop_duplicates(subset=["Invoice", "ICCID"])
    print(f"  Legacy orders: {len(df)} baris")
    return df


def load_shopee_revenue():
    files = list(RAW_DIR.glob("Order.all*.xlsx"))
    if not files:
        print("  Tidak ada file Shopee (Order.all*.xlsx)")
        return pd.DataFrame(columns=["Invoice", "Pendapatan_Shopee", "Shopee_Status"])

    print(f"  Ditemukan {len(files)} file Shopee:")
    dfs = []
    for f in files:
        print(f"  - {f.name}")
        df = pd.read_excel(f, header=0, dtype=str)
        dfs.append(pd.DataFrame({
            "Invoice":           df.iloc[:, 0],
            "SKU":               df.iloc[:, 14],
            "Pendapatan_Shopee": df.iloc[:, 17],
            "Shopee_Status":     df.iloc[:, 3],
        }))

    shopee = pd.concat(dfs, ignore_index=True)
    shopee["Invoice"] = shopee["Invoice"].str.strip().str.lstrip("'")
    shopee["SKU"] = shopee["SKU"].str.strip().str.upper().apply(
        lambda s: normalize_sku(s, SKU_MARKETPLACE_RULES)
    )
    shopee["Shopee_Status"]     = shopee["Shopee_Status"].str.strip()
    shopee["Pendapatan_Shopee"] = (
        shopee["Pendapatan_Shopee"]
        .str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )
    shopee = shopee.drop_duplicates(subset=["Invoice", "SKU"])
    need_manual = (
        (shopee["Pendapatan_Shopee"] == 0) |
        (shopee["Shopee_Status"].str.contains("Permintaan Disetujui", na=False))
    ).sum()
    print(f"  Shopee: {len(shopee)} invoice ({need_manual} perlu input manual)")
    return shopee


def load_manual_input():
    if not MANUAL_INPUT_PATH.exists():
        return pd.DataFrame(columns=["Invoice", "Harga_Final"])

    df = pd.read_excel(MANUAL_INPUT_PATH, dtype=str)
    df["Invoice"]     = df["Invoice"].str.strip().str.lstrip("'")
    df["Harga_Final"] = pd.to_numeric(df["Harga_Final"], errors="coerce")
    filled = df["Harga_Final"].notna().sum()
    print(f"  Manual input: {filled} invoice sudah diisi dari {len(df)} total")
    return df[["Invoice", "Harga_Final"]]


def generate_manual_input(orders, shopee):
    is_shopee = orders["Channel"].str.contains("Shopee", case=False, na=False)
    mask = (
        (orders["Pendapatan_Shopee"].fillna(0) == 0) |
        (orders["Shopee_Status"].str.contains("Permintaan Disetujui", na=False))
    )
    need_manual = orders[is_shopee & mask].copy()
    need_manual = need_manual.drop_duplicates(subset=["Invoice", "ICCID"], keep="first")

    if need_manual.empty:
        print("  Tidak ada invoice yang perlu input manual")
        return

    existing = pd.DataFrame(columns=["Invoice", "ICCID", "Harga_Final"])
    if MANUAL_INPUT_PATH.exists():
        existing = pd.read_excel(MANUAL_INPUT_PATH, dtype=str)
        existing["Invoice"]     = existing["Invoice"].str.strip()
        existing["ICCID"]       = existing["ICCID"].str.strip()
        existing["Harga_Final"] = pd.to_numeric(existing["Harga_Final"], errors="coerce")

    if not existing.empty:
        existing_keys = set(zip(existing["Invoice"], existing["ICCID"]))
        need_manual = need_manual[
            ~need_manual.apply(lambda r: (r["Invoice"], r["ICCID"]) in existing_keys, axis=1)
        ]

    if need_manual.empty:
        print("  Tidak ada invoice baru yang perlu input manual")
        return

    output_new = need_manual[["Channel", "Invoice", "ICCID", "SKU",
                               "Product_Name", "Order_Date",
                               "Pendapatan_Shopee", "Shopee_Status"]].copy()
    output_new["Harga_Final"] = None
    output_new["Catatan"] = output_new["Shopee_Status"].apply(
        lambda s: "Isi Harga_Final — Permintaan Disetujui (refund)"
                  if "Permintaan Disetujui" in str(s)
                  else "Isi Harga_Final — harga tidak terbaca"
    )
    output_new.insert(output_new.columns.get_loc("Pendapatan_Shopee") + 1,
                      "Harga_Final", output_new.pop("Harga_Final"))

    output = pd.concat([existing, output_new], ignore_index=True)

    with pd.ExcelWriter(MANUAL_INPUT_PATH, engine="openpyxl") as writer:
        output.to_excel(writer, sheet_name="Input Manual", index=False)
        ws = writer.sheets["Input Manual"]

        hf_col      = output.columns.get_loc("Harga_Final") + 1
        FILL_YELLOW = PatternFill("solid", fgColor="FFFF99")
        FILL_HEADER = PatternFill("solid", fgColor="2C3E50")
        FONT_HEADER = Font(color="FFFFFF", bold=True)

        for cell in ws[1]:
            cell.fill = FILL_HEADER
            cell.font = FONT_HEADER
        for row in ws.iter_rows(min_row=2):
            row[hf_col - 1].fill = FILL_YELLOW
        for col in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col), default=0)
            ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 40)

    print(f"  Manual input file: {MANUAL_INPUT_PATH} ({len(output_new)} baru, {len(existing)} existing)")


def load_tokopedia_revenue():
    files = list(RAW_DIR.glob("Semua pesanan*.xlsx"))
    if not files:
        print("  Tidak ada file Tokopedia (Semua pesanan*.xlsx)")
        return pd.DataFrame(columns=["Invoice", "Pendapatan_Tokped"])

    print(f"  Ditemukan {len(files)} file Tokopedia:")
    dfs = []
    for f in files:
        print(f"  - {f.name}")
        df = pd.read_excel(f, header=0, dtype=str)
        dfs.append(pd.DataFrame({
            "Invoice":           df.iloc[:, 0],
            "SKU":               df.iloc[:, 6],
            "Pendapatan_Tokped": df.iloc[:, 11],
        }))

    tokped = pd.concat(dfs, ignore_index=True)
    tokped["Invoice"] = tokped["Invoice"].str.strip().str.lstrip("'")
    tokped["SKU"] = tokped["SKU"].str.strip().str.upper().apply(
        lambda s: normalize_sku(s, SKU_MARKETPLACE_RULES)
    )
    tokped["Pendapatan_Tokped"] = pd.to_numeric(tokped["Pendapatan_Tokped"], errors="coerce").fillna(0)
    tokped = tokped[tokped["Pendapatan_Tokped"] > 0].drop_duplicates(subset=["Invoice", "SKU"])
    print(f"  Tokopedia: {len(tokped)} baris")
    return tokped


def load_db_data(engine):
    with engine.connect() as conn:
        country_usage = pd.read_sql(text("""
            SELECT iccid, country, SUM(country_usage_mb) as usage_mb
            FROM processed.country_usage
            GROUP BY iccid, country
        """), conn)

        final = pd.read_sql(text("""
            SELECT iccid, package, start_date, end_date,
                   total_usage_mb, total_quota_mb,
                   real_cost_cny, real_cost_idr
            FROM processed.final_output
        """), conn)

        active_sub = pd.read_sql(text("""
            SELECT iccid, package, status, start_date
            FROM raw.subscription
            WHERE LOWER(status) != 'expired'
        """), conn)

    return country_usage, final, active_sub


def apply_date_filter(df, start_date=None, end_date=None):
    """Filter DataFrame by Order_Date range. Returns filtered copy."""
    filtered = df.copy()
    if start_date:
        sd = pd.to_datetime(start_date)
        filtered = filtered[filtered["Order_Date"] >= sd]
        print(f"  Date filter: >= {start_date} ({len(df) - len(filtered)} excluded)")
    if end_date:
        ed = pd.to_datetime(end_date)
        filtered = filtered[filtered["Order_Date"] <= ed]
        remaining = len(df) - len(filtered)
        print(f"  Date filter: <= {end_date}")
    if start_date or end_date:
        print(f"  After date filter: {len(filtered)} dari {len(df)} baris")
    return filtered


def style_sheet(ws, pct_cols=(), idr_cols=(), cny_cols=(), status_col=None):
    FILL_RUGI   = PatternFill("solid", fgColor="FFCCCC")
    FILL_BAGUS  = PatternFill("solid", fgColor="CCFFCC")
    FILL_NORMAL = PatternFill("solid", fgColor="FFF9CC")
    FILL_HEADER = PatternFill("solid", fgColor="2C3E50")
    FONT_HEADER = Font(color="FFFFFF", bold=True, size=11)
    ALIGN_CTR   = Alignment(horizontal="center", vertical="center")
    STATUS_FILL = {"RUGI": FILL_RUGI, "BAGUS": FILL_BAGUS, "NORMAL": FILL_NORMAL}
    IDR_FMT = '"Rp"#,##0'
    CNY_FMT = '"¥"#,##0.00'
    PCT_FMT = '0.00%'

    for cell in ws[1]:
        cell.fill = FILL_HEADER; cell.font = FONT_HEADER; cell.alignment = ALIGN_CTR
    headers = {cell.value: cell.column for cell in ws[1]}
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            col_name = ws.cell(1, cell.column).value
            if col_name in pct_cols:
                cell.number_format = PCT_FMT; cell.alignment = ALIGN_CTR
            elif col_name in idr_cols:
                cell.number_format = IDR_FMT
            elif col_name in cny_cols:
                cell.number_format = CNY_FMT
        if status_col and status_col in headers:
            stat = ws.cell(row[0].row, headers[status_col]).value
            fill = STATUS_FILL.get(stat)
            if fill:
                for cell in row: cell.fill = fill
    for col in ws.columns:
        max_len = max((len(str(c.value or "")) for c in col), default=0)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 40)


if __name__ == "__main__":
    args = parse_args()
    OUTPUT = get_output_path(args)

    print("Loading orders...")
    orders = load_orders()
    backlog = pd.DataFrame(columns=BACKLOG_COLS + ["Backlog_Reason"])

    print("\nLoading Shopee revenue...")
    shopee = load_shopee_revenue()

    print("\nLoading Tokopedia revenue...")
    tokped = load_tokopedia_revenue()

    orders = orders.merge(shopee, on=["Invoice", "SKU"], how="left")
    orders = orders.merge(tokped, on=["Invoice", "SKU"], how="left")

    # load legacy
    print("\nLoading legacy orders...")
    legacy = load_legacy_orders()

    if not legacy.empty:
        legacy = legacy.merge(shopee[["Invoice", "SKU", "Pendapatan_Shopee", "Shopee_Status"]],
                              on=["Invoice", "SKU"], how="left")
        legacy = legacy.merge(tokped[["Invoice", "SKU", "Pendapatan_Tokped"]],
                              on=["Invoice", "SKU"], how="left")

        for col in ["Channel", "Order_Date", "Product_Name", "Product_Type", "Month"]:
            if col not in legacy.columns:
                legacy[col] = "Legacy"
        legacy["Order_Date"] = pd.to_datetime(legacy["Order_Date"], errors="coerce")
        legacy["Month"]      = legacy["Order_Date"].dt.strftime("%Y-%b").fillna("2026-Feb")

        is_shopee_leg = legacy["Pendapatan_Shopee"].fillna(0) > 0
        is_tokped_leg = legacy["Pendapatan_Tokped"].fillna(0) > 0
        legacy["Harga_Jual"] = np.select(
            [is_shopee_leg, is_tokped_leg],
            [legacy["Pendapatan_Shopee"], legacy["Pendapatan_Tokped"]],
            default=0
        )

        orders = pd.concat([orders, legacy], ignore_index=True)
        print(f"  Legacy digabung: {len(legacy)} baris")

    is_shopee = orders["Channel"].str.contains("Shopee",    case=False, na=False)
    is_tokped = orders["Channel"].str.contains("Tokopedia", case=False, na=False)
    orders["Harga_Jual"] = np.select(
        [is_shopee, is_tokped],
        [orders["Pendapatan_Shopee"].fillna(0),
         orders["Pendapatan_Tokped"].fillna(0)],
        default=orders["Harga_Jual"]
    )

    print("\nGenerating manual input file...")
    generate_manual_input(orders, shopee)

    print("\nLoading manual input...")
    has_manual = pd.Series(False, index=orders.index)

    manual = load_manual_input()
    if not manual.empty:
        orders = orders.merge(manual, on="Invoice", how="left")
        orders = orders.reset_index(drop=True)
        has_manual = orders["Harga_Final"].notna()
        orders.loc[has_manual, "Harga_Jual"] = orders.loc[has_manual, "Harga_Final"]
        orders = orders.drop(columns=["Harga_Final"])
        print(f"  Manual input diterapkan: {has_manual.sum()} invoice")

    has_manual = has_manual.reset_index(drop=True) if not manual.empty else pd.Series(False, index=orders.index)

    bl_refund = orders[has_manual & (orders["Harga_Jual"] == 0)][BACKLOG_COLS].copy()
    bl_refund["Backlog_Reason"] = "FULL_REFUND"

    bl_zero = orders[~has_manual & (orders["Harga_Jual"] == 0)][BACKLOG_COLS].copy()
    bl_zero["Backlog_Reason"] = "HARGA_0_SETELAH_MERGE"
    backlog = pd.concat([bl_refund, bl_zero], ignore_index=True)

    print("\nLoading data dari DB...")
    engine                           = get_engine()
    country_usage, final, active_sub = load_db_data(engine)

    print("\nMatching orders ke subscriptions...")
    from processors.cost_calculator import match_orders_to_subscriptions
    orders, diagnostics_df = match_orders_to_subscriptions(orders, final, active_sub)

    # pisahkan yang tidak ter-match → backlog
    no_match = orders[orders["real_cost_idr"].isna()].copy()
    orders   = orders[orders["real_cost_idr"].notna() & (orders["real_cost_idr"] > 0)].copy()
    orders   = orders[orders["Harga_Jual"] > 0].copy()

    if not no_match.empty:
        bl_nomatch = no_match[BACKLOG_COLS + ["Match_Method"]].copy()
        bl_nomatch = bl_nomatch.rename(columns={"Match_Method": "Backlog_Reason"})
        backlog    = pd.concat([backlog, bl_nomatch], ignore_index=True)

        backlog_db = no_match[["Channel", "Invoice", "ICCID", "SKU", "Product_Name",
                                "Product_Type", "Order_Date", "Harga_Jual", "Match_Method"]].copy()
        backlog_db.columns = ["channel", "invoice", "iccid", "sku", "product_name",
                               "product_type", "order_date", "harga_jual", "reason"]
        backlog_db["resolved"] = False

        with engine.begin() as conn:
            for _, row in backlog_db.iterrows():
                exists = conn.execute(text("""
                    SELECT 1 FROM processed.margin_backlog
                    WHERE invoice = :inv AND iccid = :iccid
                """), {"inv": row["invoice"], "iccid": row["iccid"]}).fetchone()
                if not exists:
                    conn.execute(text("""
                        INSERT INTO processed.margin_backlog
                        (channel, invoice, iccid, sku, product_name,
                         product_type, order_date, harga_jual, reason, resolved)
                        VALUES (:channel, :invoice, :iccid, :sku, :product_name,
                                :product_type, :order_date, :harga_jual, :reason, :resolved)
                    """), row.to_dict())
        print(f"  Backlog DB: {len(backlog_db)} baris disimpan")

    print(f"  Excluded: {len(no_match)} baris (masuk backlog)")

    # cek backlog lama yang sudah bisa di-resolve
    with engine.connect() as conn:
        old_backlog = pd.read_sql(text("""
            SELECT id, invoice, iccid, sku
            FROM processed.margin_backlog
            WHERE resolved = FALSE AND reason IN ('ACTIVE', 'NO_ORDER_DATA')
        """), conn)

    if not old_backlog.empty:
        from processors.behaviour_factor import build_sku
        final_sku = final.copy()
        final_sku["sku_match"] = final_sku["package"].apply(lambda p: build_sku(p).upper())
        resolvable = old_backlog.merge(
            final_sku[["iccid", "sku_match", "real_cost_idr"]],
            left_on=["iccid", "sku"], right_on=["iccid", "sku_match"], how="inner"
        )
        resolvable = resolvable[resolvable["real_cost_idr"].notna()]
        if not resolvable.empty:
            with engine.begin() as conn:
                for bid in resolvable["id"].tolist():
                    conn.execute(text("""
                        UPDATE processed.margin_backlog SET resolved = TRUE WHERE id = :id
                    """), {"id": bid})
            print(f"  Backlog resolved: {len(resolvable)} baris sudah bisa dihitung")

    # ── NEGARA DETAIL ─────────────────────────────────────────
    country_usage["Rate_CNY"]     = country_usage["country"].apply(get_rate)
    country_usage["Cost_CNY"]     = country_usage["usage_mb"] / 1024 * country_usage["Rate_CNY"]
    iccid_total                   = country_usage.groupby("iccid")["usage_mb"].transform("sum")
    country_usage["usage_pct"]    = (country_usage["usage_mb"] / iccid_total * 100).round(1)
    country_usage["country_name"] = country_usage["country"].apply(
        lambda x: MCC_MAP.get(str(extract_mcc(x)), x)
    )
    country_detail = (
        country_usage
        .sort_values(["iccid", "usage_pct"], ascending=[True, False])
        .groupby("iccid")
        .apply(lambda d: ", ".join(
            f"{row['country_name']} ({row['usage_pct']}%)"
            for _, row in d.iterrows()
        ), include_groups=False)
        .reset_index()
    )
    country_detail.columns = ["iccid", "Negara_Detail"]

    # ── BUILD REPORT ──────────────────────────────────────────
    df = orders.copy()
    df = df.rename(columns={
        "real_cost_cny":  "Real_Cost_CNY",
        "real_cost_idr":  "Real_Cost_IDR",
        "total_usage_mb": "Total_Usage_MB",
        "total_quota_mb": "Total_Quota_MB",
    })
    df["Real_Cost_CNY"] = df["Real_Cost_CNY"].fillna(0).round(2)
    df["Real_Cost_IDR"] = df["Real_Cost_IDR"].fillna(0).round(2)
    df["Total_Usage_Display"] = df["Total_Usage_MB"].apply(
        lambda x: f"{round(x/1024, 2)} GB" if pd.notna(x) and x >= 1024
        else f"{round(x, 2)} MB" if pd.notna(x) else "-"
    )
    df["Total_Quota_Display"] = df["Total_Quota_MB"].apply(
        lambda x: f"{round(x/1024, 2)} GB" if pd.notna(x) and x >= 1024
        else f"{round(x, 2)} MB" if pd.notna(x) else "-"
    )
    df = df.merge(country_detail.rename(columns={"iccid": "ICCID"}), on="ICCID", how="left")

    df["Komisi_Pct"] = df.apply(lambda r: get_commission(r["Channel"], r["Product_Type"]), axis=1)
    df["Komisi_IDR"]  = (df["Harga_Jual"] * df["Komisi_Pct"]).round(2)
    df["Net_Revenue"] = (df["Harga_Jual"] - df["Komisi_IDR"]).round(2)
    df["Margin_IDR"]  = (df["Net_Revenue"] - df["Real_Cost_IDR"]).round(2)
    df["Margin_Pct"]  = np.where(
        df["Net_Revenue"] > 0,
        (df["Margin_IDR"] / df["Net_Revenue"]).round(4), 0
    )
    df["Status"] = np.select(
        [df["Margin_Pct"] < 0, df["Margin_Pct"] >= 0.30],
        ["RUGI", "BAGUS"], default="NORMAL"
    )

    # ── APPLY DATE FILTER (output only) ───────────────────────
    df_all = df.copy()  # keep full data for total context
    if args.start_date or args.end_date:
        print(f"\nApplying date filter for output...")
        df = apply_date_filter(df, args.start_date, args.end_date)
        backlog = apply_date_filter(backlog, args.start_date, args.end_date)

    # ── SUMMARIES ─────────────────────────────────────────────
    summary_sku = (
        df.groupby("SKU")
        .agg(
            Total_Invoice     = ("Invoice",       "nunique"),
            Total_ICCID       = ("ICCID",         "count"),
            Total_Pendapatan  = ("Harga_Jual",    "sum"),
            Total_Komisi      = ("Komisi_IDR",    "sum"),
            Total_Net_Revenue = ("Net_Revenue",   "sum"),
            Total_Cost_IDR    = ("Real_Cost_IDR", "sum"),
            Total_Margin_IDR  = ("Margin_IDR",    "sum"),
        )
        .reset_index()
    )
    for col in ["Total_Pendapatan", "Total_Komisi", "Total_Net_Revenue",
                "Total_Cost_IDR", "Total_Margin_IDR"]:
        summary_sku[col] = summary_sku[col].round(2)
    summary_sku["Avg_Margin_Pct"] = (
        summary_sku["Total_Margin_IDR"] / summary_sku["Total_Net_Revenue"].replace(0, np.nan)
    ).round(4)
    summary_sku["Status"] = np.select(
        [summary_sku["Avg_Margin_Pct"] < 0, summary_sku["Avg_Margin_Pct"] >= 0.30],
        ["RUGI", "BAGUS"], default="NORMAL"
    )
    summary_sku = summary_sku.sort_values("Avg_Margin_Pct")

    summary_month = (
        df.groupby("Month")
        .agg(
            Total_Invoice          = ("Invoice",       "nunique"),
            Total_ICCID            = ("ICCID",         "count"),
            Total_Pendapatan_Gross = ("Harga_Jual",    "sum"),
            Total_Komisi           = ("Komisi_IDR",    "sum"),
            Total_Net_Revenue      = ("Net_Revenue",   "sum"),
            Total_Cost_IDR         = ("Real_Cost_IDR", "sum"),
            Total_Margin_IDR       = ("Margin_IDR",    "sum"),
            ICCID_Rugi             = ("Status",        lambda x: (x == "RUGI").sum()),
            ICCID_Normal           = ("Status",        lambda x: (x == "NORMAL").sum()),
            ICCID_Bagus            = ("Status",        lambda x: (x == "BAGUS").sum()),
        )
        .reset_index()
    )
    for col in ["Total_Pendapatan_Gross", "Total_Komisi", "Total_Net_Revenue",
                "Total_Cost_IDR", "Total_Margin_IDR"]:
        summary_month[col] = summary_month[col].round(2)
    summary_month["Avg_Margin_Pct"] = (
        summary_month["Total_Margin_IDR"] / summary_month["Total_Net_Revenue"].replace(0, np.nan)
    ).round(4)
    summary_month = summary_month.sort_values("Month")

    total_margin_idr  = round(df["Margin_IDR"].sum(), 2)
    total_net_revenue = round(df["Net_Revenue"].sum(), 2)
    total = pd.DataFrame([{
        "Total_Invoice":          df["Invoice"].nunique(),
        "Total_ICCID":            len(df),
        "Total_Pendapatan_Gross": round(df["Harga_Jual"].sum(), 2),
        "Total_Komisi":           round(df["Komisi_IDR"].sum(), 2),
        "Total_Net_Revenue":      total_net_revenue,
        "Total_Cost_IDR":         round(df["Real_Cost_IDR"].sum(), 2),
        "Total_Margin_IDR":       total_margin_idr,
        "Avg_Margin_Pct":         round(total_margin_idr / total_net_revenue, 4) if total_net_revenue else 0,
        "ICCID_Rugi":             (df["Status"] == "RUGI").sum(),
        "ICCID_Normal":           (df["Status"] == "NORMAL").sum(),
        "ICCID_Bagus":            (df["Status"] == "BAGUS").sum(),
        "Total_Backlog":          len(backlog),
    }])

    # add date range info
    if args.start_date or args.end_date:
        total["Date_Range"] = f"{args.start_date or 'awal'} s/d {args.end_date or 'akhir'}"

    print("\nSUMMARY TOTAL:")
    print(total.to_string(index=False))

    # ── SAVE TO EXCEL ─────────────────────────────────────────
    print(f"\nSaving to {OUTPUT}...")

    with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
        detail_cols = [
            "Channel", "Product_Name", "Invoice", "ICCID", "SKU", "Product_Type", "Month",
            "Harga_Jual", "Komisi_IDR", "Net_Revenue",
            "Real_Cost_CNY", "Real_Cost_IDR",
            "Total_Usage_Display", "Total_Quota_Display",
            "Margin_IDR", "Margin_Pct", "Status", "Match_Method",
            "Negara_Detail"
        ]
        if "Match_Method" not in df.columns:
            df["Match_Method"] = "SKU_MATCH"

        df[detail_cols].to_excel(writer, sheet_name="Detail per ICCID", index=False)
        summary_sku.to_excel(   writer, sheet_name="Summary per SKU",   index=False)
        summary_month.to_excel( writer, sheet_name="Summary per Bulan", index=False)
        total.to_excel(         writer, sheet_name="Summary Total",      index=False)
        backlog.to_excel(       writer, sheet_name="Backlog",            index=False)

        # diagnostic sheet — untuk review NO_ORDER_DATA
        if not diagnostics_df.empty:
            diagnostics_df.to_excel(writer, sheet_name="Diagnostics", index=False)

        wb = writer.book
        IDR_COLS = ["Harga_Jual", "Komisi_IDR", "Net_Revenue", "Real_Cost_IDR",
                    "Margin_IDR", "Total_Pendapatan", "Total_Komisi", "Total_Net_Revenue",
                    "Total_Cost_IDR", "Total_Margin_IDR", "Total_Pendapatan_Gross"]
        PCT_COLS = ["Margin_Pct", "Avg_Margin_Pct"]
        CNY_COLS = ["Real_Cost_CNY"]

        style_sheet(wb["Detail per ICCID"], pct_cols=PCT_COLS, idr_cols=IDR_COLS,
                    cny_cols=CNY_COLS, status_col="Status")
        style_sheet(wb["Summary per SKU"],  pct_cols=PCT_COLS, idr_cols=IDR_COLS,
                    status_col="Status")
        style_sheet(wb["Summary per Bulan"], pct_cols=PCT_COLS, idr_cols=IDR_COLS)
        style_sheet(wb["Summary Total"],     pct_cols=PCT_COLS, idr_cols=IDR_COLS)

        if not diagnostics_df.empty:
            ws_diag = wb["Diagnostics"]
            FILL_HEADER = PatternFill("solid", fgColor="2C3E50")
            FONT_HEADER = Font(color="FFFFFF", bold=True, size=11)
            FILL_WARN   = PatternFill("solid", fgColor="FFDDDD")
            for cell in ws_diag[1]:
                cell.fill = FILL_HEADER; cell.font = FONT_HEADER
            for row in ws_diag.iter_rows(min_row=2):
                for cell in row: cell.fill = FILL_WARN
            for col in ws_diag.columns:
                max_len = max((len(str(c.value or "")) for c in col), default=0)
                ws_diag.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 60)

    print(f"\nMargin report selesai! → {OUTPUT}")
    if args.start_date or args.end_date:
        print(f"  Date range: {args.start_date or 'awal'} s/d {args.end_date or 'akhir'}")
    if not diagnostics_df.empty:
        print(f"  Diagnostics: {len(diagnostics_df)} ICCID perlu di-review (lihat sheet Diagnostics)")