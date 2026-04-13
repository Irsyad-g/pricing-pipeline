import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


def _load_one(f):
    print(f"Load SUB: {f.name}")
    return pd.read_excel(f, dtype=str)


def load_subscription(sub_files):
    sub_list = [None] * len(sub_files)

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(_load_one, f): i for i, f in enumerate(sub_files)}
        for future in as_completed(futures):
            i = futures[future]
            sub_list[i] = future.result()

    sub = pd.concat(sub_list, ignore_index=True)

    sub["ICCID"]     = sub.iloc[:, 8].astype(str).str.replace(r"\D+", "", regex=True)
    sub["PACKAGE"]   = sub.iloc[:, 5]
    sub["STATUS"]    = sub.iloc[:, 9]
    sub["START_RAW"] = sub.iloc[:, 14]
    sub["END_RAW"]   = sub.iloc[:, 15]

    # simpan semua status, filter expired dilakukan di processor
    sub["START"] = pd.to_datetime(sub["START_RAW"], errors="coerce")
    sub["END"]   = pd.to_datetime(sub["END_RAW"],   errors="coerce")
    sub["DAYS"]  = (sub["END"] - sub["START"]).dt.days + 1

    sub = sub.dropna(subset=["ICCID", "PACKAGE", "START"])
    sub.drop_duplicates(inplace=True)
    sub.reset_index(drop=True, inplace=True)

    sub["PACKAGE"] = sub["PACKAGE"].astype("category")

    expired = (sub["STATUS"].str.lower() == "expired").sum()
    active  = (sub["STATUS"].str.lower() != "expired").sum()
    print(f"  Loaded: {expired} expired, {active} active/lainnya")

    return sub