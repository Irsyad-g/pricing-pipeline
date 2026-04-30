"""
ICCID Checker Web App
=====================
Local LAN web app for checking roaming usage per ICCID.
Run: python app.py
Access: http://<your-local-ip>:5000
"""

import json
import re
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
import os

load_dotenv()

app = Flask(__name__)

_HERE     = Path(__file__).resolve().parent
_PROJECT  = _HERE.parent
MCC_MAP_PATH      = _PROJECT / "data/mappings/mcc_map.json"
COUNTRY_RATE_PATH = _PROJECT / "data/mappings/country_rate.json"

with open(MCC_MAP_PATH) as f:
    MCC_MAP = json.load(f)
with open(COUNTRY_RATE_PATH) as f:
    COUNTRY_RATE = json.load(f)

_RATE_VALUES = list(COUNTRY_RATE.values())
GLOBAL_RATE  = sorted(_RATE_VALUES)[len(_RATE_VALUES) // 2]

RATE_CNY_TO_IDR = 2650

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "roaming"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

SCHEMA_PROCESSED = os.getenv("SCHEMA_PROCESSED", "processed")
SCHEMA_RAW       = os.getenv("SCHEMA_RAW",       "raw")
TABLE_FINAL      = os.getenv("TABLE_FINAL",      "final_output")
TABLE_COUNTRY    = os.getenv("TABLE_COUNTRY",    "country_usage")
TABLE_SUB        = os.getenv("TABLE_SUB",        "subscription")


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _resolve_country_area(area_str):
    """
    Parse country field from processed.country_usage.
    Format: "52010 Indonesia"  (MCC+MNC prefix, space, country name)
    Returns: (country_name, rate_cny_per_gb, mcc_prefix)
    """
    s = str(area_str).strip()
    parts = s.split(" ", 1)

    if len(parts) == 2 and parts[0].isdigit():
        # Country name is embedded after the MCC prefix — use it directly
        country_name = parts[1].strip()
        rate = COUNTRY_RATE.get(country_name, GLOBAL_RATE)
        return country_name, rate, parts[0]

    # Fallback: pure digit string → try 3-digit MCC lookup
    m = re.match(r"(\d{3})", s)
    if m:
        country_name = MCC_MAP.get(m.group(1), s)
        rate = COUNTRY_RATE.get(country_name, GLOBAL_RATE)
        return country_name, rate, m.group(1)

    return s, GLOBAL_RATE, ""


def _query_iccid(cur, iccid: str) -> dict:
    # ── 1. Subscription summary from processed.final_output ──────────────
    cur.execute(f"""
        SELECT
            iccid,
            package,
            start_date,
            end_date,
            COALESCE(total_quota_mb, 0) AS total_quota_mb,
            COALESCE(total_usage_mb, 0) AS total_usage_mb,
            COALESCE(real_cost_cny, 0)  AS real_cost_cny,
            COALESCE(real_cost_idr, 0)  AS real_cost_idr
        FROM {SCHEMA_PROCESSED}.{TABLE_FINAL}
        WHERE iccid = %s
        ORDER BY start_date DESC
    """, (iccid,))
    subs = cur.fetchall()

    if not subs:
        return {"iccid": iccid, "found": False}

    # ── 2. Active status from raw.subscription (best-effort) ─────────────
    status    = "unknown"
    is_active = False
    try:
        cur.execute(f"""
            SELECT status
            FROM {SCHEMA_RAW}.{TABLE_SUB}
            WHERE iccid = %s
            ORDER BY start_date DESC
            LIMIT 1
        """, (iccid,))
        row = cur.fetchone()
        if row:
            status    = (row["status"] or "unknown").lower()
            is_active = status not in ("expired", "terminated", "inactive")
    except Exception:
        pass  # table may not exist — continue without status

    # ── 3. Country breakdown from processed.country_usage ─────────────────
    cur.execute(f"""
        SELECT
            country,
            SUM(country_usage_mb) AS usage_mb
        FROM {SCHEMA_PROCESSED}.{TABLE_COUNTRY}
        WHERE iccid = %s
        GROUP BY country
        ORDER BY usage_mb DESC
    """, (iccid,))
    country_rows = cur.fetchall()

    # ── 4. Aggregate totals from final_output ────────────────────────────
    total_usage_mb = sum(float(r["total_usage_mb"] or 0) for r in subs)
    total_quota_mb = sum(float(r["total_quota_mb"] or 0) for r in subs)
    total_cost_idr = sum(float(r["real_cost_idr"]  or 0) for r in subs)
    total_cost_cny = sum(float(r["real_cost_cny"]  or 0) for r in subs)

    # ── 5. Enrich countries — proportional cost from real_cost_idr ───────
    # Rate-based per-country cost doesn't match pipeline's actual calculation.
    # Distribute real_cost_idr proportionally by usage share instead.
    total_country_mb = sum(float(r["usage_mb"] or 0) for r in country_rows)

    countries = []
    for r in country_rows:
        country_name, rate, mcc_prefix = _resolve_country_area(r["country"])
        usage_mb = float(r["usage_mb"] or 0)
        share    = usage_mb / total_usage_mb if total_usage_mb > 0 else 0
        cost_idr = share * total_cost_idr
        cost_cny = share * total_cost_cny
        countries.append({
            "country":         country_name,
            "area_raw":        r["country"],
            "mcc_prefix":      mcc_prefix,
            "usage_mb":        round(usage_mb, 2),
            "usage_gb":        round(usage_mb / 1024, 4),
            "cost_idr":        round(cost_idr, 2),
            "cost_cny":        round(cost_cny, 4),
            "rate_cny_per_gb": round(rate, 4),
            "pct_of_total":    round(
                usage_mb / total_country_mb * 100 if total_country_mb else 0, 1
            ),
        })

    subscriptions = [
        {
            "package":        r["package"],
            "start_date":     str(r["start_date"])[:10] if r["start_date"] else None,
            "end_date":       str(r["end_date"])[:10]   if r["end_date"]   else None,
            "total_quota_mb": round(float(r["total_quota_mb"] or 0), 2),
            "total_quota_gb": round(float(r["total_quota_mb"] or 0) / 1024, 4),
            "total_usage_mb": round(float(r["total_usage_mb"] or 0), 2),
            "total_usage_gb": round(float(r["total_usage_mb"] or 0) / 1024, 4),
            "real_cost_cny":  round(float(r["real_cost_cny"] or 0), 4),
            "real_cost_idr":  round(float(r["real_cost_idr"] or 0), 2),
        }
        for r in subs
    ]

    return {
        "iccid":              iccid,
        "found":              True,
        "status":             status,
        "is_active":          is_active,
        "total_usage_mb":     round(total_usage_mb, 2),
        "total_usage_gb":     round(total_usage_mb / 1024, 4),
        "total_quota_mb":     round(total_quota_mb, 2),
        "total_quota_gb":     round(total_quota_mb / 1024, 4),
        "total_cost_idr":     round(total_cost_idr, 2),
        "total_cost_cny":     round(total_cost_cny, 4),
        "subscription_count": len(subscriptions),
        "subscriptions":      subscriptions,
        "country_breakdown":  countries,
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/query", methods=["POST"])
def query():
    data      = request.get_json(silent=True) or {}
    raw_input = data.get("iccids", "")

    iccids = [
        i.strip()
        for i in re.split(r"[\n,;]+", raw_input)
        if i.strip()
    ]
    iccids = list(dict.fromkeys(iccids))

    if not iccids:
        return jsonify({"error": "No ICCIDs provided"}), 400
    if len(iccids) > 50:
        return jsonify({"error": "Max 50 ICCIDs per query"}), 400

    results = []
    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for iccid in iccids:
            results.append(_query_iccid(cur, iccid))
        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        return jsonify({"error": f"DB connection failed: {e}"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"results": results, "count": len(results)})


@app.route("/api/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok", "db": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "db": str(e)}), 503


if __name__ == "__main__":
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 5000))
    print(f"\n ICCID Checker running → http://{host}:{port}")
    print(f"  Team access → http://<your-ip>:{port}")
    print("  Find your IP: ipconfig (look for IPv4 Address)\n")
    app.run(host=host, port=port, debug=False)
