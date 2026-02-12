import os, calendar, datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values

BASE = "https://api.frankfurter.app"  # Frankfurter public API :contentReference[oaicite:3]{index=3}

def month_range(year: int, month: int):
    start = dt.date(year, month, 1)
    end = dt.date(year, month, calendar.monthrange(year, month)[1])
    return start, end

def fetch_month_timeseries(year: int, month: int):
    start, end = month_range(year, month)
    url = f"{BASE}/{start.isoformat()}..{end.isoformat()}"
    data = requests.get(url, timeout=30).json()
    rates_by_date = data.get("rates", {})
    if not rates_by_date:
        raise RuntimeError(f"No rates returned for {year}-{month:02d}")
    return rates_by_date  # {"YYYY-MM-DD": {"USD":..., "DKK":...}, ...}

def compute_month_avg(rates_by_date: dict, year: int, month: int):
    sums, counts = {}, {}
    for _, rates in rates_by_date.items():
        for cur, v in rates.items():
            sums[cur] = sums.get(cur, 0.0) + float(v)
            counts[cur] = counts.get(cur, 0) + 1

    # همیشه EUR=1.0 داشته باش که join برای کشورهای یورویی قطع نشه
    sums["EUR"] = 1.0
    counts["EUR"] = 1

    month_sk = dt.date(year, month, 1)
    rows = []
    for cur in sorted(sums.keys()):
        avg = sums[cur] / counts[cur]
        rows.append((month_sk, cur, avg, counts[cur], "frankfurter/ecb"))
    return rows

def upsert_monthly(conn, rows):
    sql = """
    INSERT INTO warehouse.fx_rates_monthly
      (month_sk, currency_code, avg_rate_to_eur, obs_count, source)
    VALUES %s
    ON CONFLICT (month_sk, currency_code) DO UPDATE
    SET avg_rate_to_eur = EXCLUDED.avg_rate_to_eur,
        obs_count = EXCLUDED.obs_count,
        source = EXCLUDED.source;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

if __name__ == "__main__":
    year = int(os.getenv("FX_YEAR", "2026"))
    month = int(os.getenv("FX_MONTH", "1"))

    ts = fetch_month_timeseries(year, month)
    rows = compute_month_avg(ts, year, month)

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    try:
        upsert_monthly(conn, rows)
        print(f"Upserted {len(rows)} currencies for {year}-{month:02d}")
    finally:
        conn.close()