import re
import pandas as pd

def extract_quota(pkg, days):
    pkg  = str(pkg).upper()
    
    # guard days NaN
    if pd.isna(days) or days <= 0:
        return 0
    
    days = int(days)

    if "UNLIMITED" in pkg or "/DAYS" in pkg:
        # FUP — GB/Days (support decimal 1.5GB/Days)
        m_gb = re.search(r"(\d+\.?\d*)\s*GB\/DAYS?", pkg)
        if m_gb:
            gb_per_day = float(m_gb.group(1))
            return round(gb_per_day * 1024 * days)

        # FUP — MB/Days
        m_mb = re.search(r"(\d+\.?\d*)\s*MB\/DAYS?", pkg)
        if m_mb:
            mb_per_day = float(m_mb.group(1))
            return round(mb_per_day * days)

        # FUP — MB tanpa /Days
        if "500MB" in pkg: return round(500 * days)
        if "800MB" in pkg: return round(800 * days)
        if "300MB" in pkg: return round(300 * days)

        # Pure Unlimited
        return 10 * 1024 * days

    # BIG DATA — fixed GB
    gb = re.search(r"(\d+\.?\d*)\s*GB", pkg)
    if gb:
        return round(float(gb.group(1)) * 1024)

    return 0