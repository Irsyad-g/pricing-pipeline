import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


def _load_one(f):
    print(f"Load DAILY: {f.name}")
    df = pd.read_excel(f, dtype=str)

    # proses per file sebelum concat → hemat memory
    df["ICCID"]     = df.iloc[:, 3].astype(str).str.replace(r"\D+", "", regex=True)
    df["AREA"]      = df.iloc[:, 6]
    df["USAGE_RAW"] = df.iloc[:, 8]
    df["DATE_RAW"]  = df.iloc[:, 9]

    df["DATE"]      = pd.to_datetime(df["DATE_RAW"], errors="coerce")
    df["DATE_ONLY"] = df["DATE"].dt.normalize()

    df["USAGE_MB"]  = (
        df["USAGE_RAW"].astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    ) / 1024

    # buang kolom raw & kolom asli yang tidak dipakai → kurangi memory sebelum concat
    keep = ["ICCID", "AREA", "DATE", "DATE_ONLY", "USAGE_MB"]
    df = df[keep]

    # buang baris invalid sebelum concat
    df = df.dropna(subset=["ICCID", "DATE", "USAGE_MB"])
    df = df[df["USAGE_MB"] >= 0]

    return df


def load_daily_usage(daily_files):
    # ── 1. Baca & proses semua file paralel ─────────────────
    daily_list = [None] * len(daily_files)

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(_load_one, f): i for i, f in enumerate(daily_files)}
        for future in as_completed(futures):
            i = futures[future]
            daily_list[i] = future.result()

    # ── 2. Concat + dedup ────────────────────────────────────
    daily = pd.concat(daily_list, ignore_index=True)
    print(f"  📊 Sebelum dedup: {len(daily)} baris")

    daily = (
        daily
        .sort_values("USAGE_MB", ascending=False)
        .drop_duplicates(subset=["ICCID", "DATE", "AREA"], keep="first")
        .reset_index(drop=True)
    )
    print(f"  📊 Sesudah dedup: {len(daily)} baris")

    # ── 3. Optimize tipe data → kurangi memory usage ─────────
    daily["USAGE_MB"] = daily["USAGE_MB"].astype("float32")

    return daily