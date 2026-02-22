import os
import calendar
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values

FRANKFURTER_BASE = "https://api.frankfurter.app"
EXHOST_BASE = "https://api.exchangerate.host"

EURO_CURRENCY = "EUR"

COUNTRY_TO_CURRENCY = {
    "BG": "BGN",
    "DK": "DKK",
    "HU": "HUF",
    "RO": "RON",
    "SE": "SEK",
    "NO": "NOK",
    "RS": "RSD",
    "CO": "COP",
    "PL": "PLN",
    "ES": "EUR",
    "FI": "EUR",
    "HR": "EUR",
    "IT": "EUR",
    "LT": "EUR",
}


def month_range(year: int, month: int):
    start = dt.date(year, month, 1)
    end = dt.date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def frankfurter_supported_currencies():
    try:
        r = requests.get(f"{FRANKFURTER_BASE}/currencies", timeout=30)
        r.raise_for_status()
        return set(r.json().keys())
    except Exception as e:
        print(f"WARNING: Could not fetch Frankfurter supported currencies: {e}")
        return None


def fetch_month_timeseries_frankfurter(year: int, month: int):
    start, end = month_range(year, month)
    url = f"{FRANKFURTER_BASE}/{start.isoformat()}..{end.isoformat()}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    rates_by_date = data.get("rates", {})
    if not rates_by_date:
        return {}

    return rates_by_date


def fetch_month_timeseries_exhost(year: int, month: int, symbols: list[str]):
    start, end = month_range(year, month)
    sym = ",".join(symbols)

    url = (
        f"{EXHOST_BASE}/timeseries"
        f"?start_date={start.isoformat()}&end_date={end.isoformat()}"
        f"&base={EURO_CURRENCY}&symbols={sym}"
    )

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        rates_by_date = data.get("rates", {})
        if not rates_by_date:
            print(f"WARNING: exchangerate.host returned no rates for {year}-{month:02d}")
            return {}

        return rates_by_date

    except Exception as e:
        print(f"WARNING: exchangerate.host request failed for {year}-{month:02d}: {e}")
        return {}


def compute_month_avg_currency_to_eur(rates_by_date: dict, year: int, month: int):
    sums = {}
    counts = {}

    for _, rates in rates_by_date.items():
        for cur, v in rates.items():
            sums[cur] = sums.get(cur, 0.0) + float(v)
            counts[cur] = counts.get(cur, 0) + 1

    # Ensure EUR always exists
    sums[EURO_CURRENCY] = 1.0
    counts[EURO_CURRENCY] = 1

    month_sk = dt.date(year, month, 1)
    rows = []

    for cur in sorted(sums.keys()):
        avg_eur_to_cur = sums[cur] / counts[cur]   # 1 EUR = X CUR
        avg_cur_to_eur = 1.0 / avg_eur_to_cur      # 1 CUR = Y EUR

        rows.append((month_sk, cur, avg_cur_to_eur, counts[cur], "frankfurter_or_exhost"))

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


def run_year_range(conn, start_year: int, end_year: int, country_codes: list[str]):
    needed_currencies = sorted(
        {COUNTRY_TO_CURRENCY[c] for c in country_codes if c in COUNTRY_TO_CURRENCY}
    )

    non_eur = [c for c in needed_currencies if c != EURO_CURRENCY]

    ff_supported = frankfurter_supported_currencies()

    failed_months = []
    total_rows = 0

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            print(f"Processing {year}-{month:02d}...")

            rates_by_date = {}

            # Determine which currencies to request from Frankfurter
            if ff_supported is not None:
                ff_symbols = [s for s in non_eur if s in ff_supported]
                ex_symbols = [s for s in non_eur if s not in ff_supported]
            else:
                ff_symbols = non_eur
                ex_symbols = []

            # Fetch from Frankfurter
            if ff_symbols:
                ff_data = fetch_month_timeseries_frankfurter(year, month)
                for d, rmap in ff_data.items():
                    rates_by_date.setdefault(d, {})
                    for s in ff_symbols:
                        if s in rmap:
                            rates_by_date[d][s] = rmap[s]

            # Detect missing symbols (not found in Frankfurter data)
            present_any = set()
            for _, rmap in rates_by_date.items():
                present_any.update(rmap.keys())

            missing = [s for s in non_eur if s not in present_any]

            # Add explicitly unsupported currencies
            for s in ex_symbols:
                if s not in missing:
                    missing.append(s)

            if missing:
                print(f"INFO: Missing currencies for {year}-{month:02d}: {missing}")

                ex_data = fetch_month_timeseries_exhost(year, month, missing)
                for d, rmap in ex_data.items():
                    rates_by_date.setdefault(d, {})
                    for s in missing:
                        if s in rmap:
                            rates_by_date[d][s] = rmap[s]

            if not rates_by_date:
                print(f"WARNING: No FX rates found at all for {year}-{month:02d}, skipping.")
                failed_months.append(f"{year}-{month:02d}")
                continue

            rows = compute_month_avg_currency_to_eur(rates_by_date, year, month)

            try:
                upsert_monthly(conn, rows)
                total_rows += len(rows)
                print(f"Upserted {len(rows)} currencies for {year}-{month:02d}")
            except Exception as e:
                print(f"ERROR: DB upsert failed for {year}-{month:02d}: {e}")
                failed_months.append(f"{year}-{month:02d}")

    print("\n==================== SUMMARY ====================")
    print(f"Total upserted rows: {total_rows}")

    if failed_months:
        print("Months that failed or were skipped:")
        for m in failed_months:
            print(" -", m)
    else:
        print("All months processed successfully.")

    return total_rows


if __name__ == "__main__":
    start_year = int(os.getenv("FX_START_YEAR", "2024"))
    end_year = int(os.getenv("FX_END_YEAR", "2025"))

    country_codes = os.getenv(
        "FX_COUNTRIES",
        "BG,CO,DK,ES,FI,HR,HU,IT,LT,NO,PL,RO,RS,SE"
    ).split(",")

    country_codes = [c.strip().upper() for c in country_codes if c.strip()]
    country_codes = ["PL" if c == "PO" else c for c in country_codes]

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

    try:
        run_year_range(conn, start_year, end_year, country_codes)
    finally:
        conn.close()