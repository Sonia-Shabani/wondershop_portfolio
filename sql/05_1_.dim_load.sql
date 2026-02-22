/* ============================================================
   Load Dimensions (Idempotent / Safe to run repeatedly)

   - dim_country : static lookup table (country & currency)
   - dim_date    : calendar date dimension
   - dim_product : SCD Type 1 (keeps latest product attributes)

   This script can be executed multiple times without
   breaking surrogate keys or creating duplicates.
   ============================================================ */


-- ============================================================
-- 1) dim_country
-- Purpose:
--   Reference table for countries and their currencies.
-- Characteristics:
--   - Small, mostly static lookup table
--   - One row per country
-- Method:
--   UPSERT based on country_name.
--   If a country already exists, its codes are updated.
-- ============================================================
INSERT INTO warehouse.dim_country (country_name, country_code, currency_code) VALUES
('Bulgaria','BG','BGN'),
('Colombia','CO','COP'),
('Croatia','HR','EUR'),
('Denmark','DK','DKK'),
('Finland','FI','EUR'),
('Hungary','HU','HUF'),
('Italy','IT','EUR'),
('Lithuania','LT','EUR'),
('Norway','NO','NOK'),
('Poland','PL','PLN'),
('Romania','RO','RON'),
('Serbia','RS','RSD'),
('Spain','ES','EUR'),
('Sweden','SE','SEK')
ON CONFLICT (country_name) DO UPDATE
SET country_code  = EXCLUDED.country_code,
    currency_code = EXCLUDED.currency_code;


-- ============================================================
-- 2) dim_date
-- Purpose:
--   Calendar dimension used for time-based reporting
--   (year, quarter, month).
-- Characteristics:
--   - One row per calendar day
--   - Supports grouping by month/quarter/year
-- Method:
--   - Generate a continuous date range using generate_series
--   - Insert once; ignore duplicates on re-runs
-- ============================================================
INSERT INTO warehouse.dim_date (date_sk, year, quarter, month_number, year_month)
SELECT
  d::date                               AS date_sk,
  EXTRACT(YEAR FROM d)::int             AS year,
  EXTRACT(QUARTER FROM d)::int          AS quarter,
  EXTRACT(MONTH FROM d)::int            AS month_number,
  TO_CHAR(d,'YYYY-MM')                  AS year_month
FROM generate_series(
       '2023-01-01'::date,
       '2026-12-31'::date,
       '1 day'::interval
     ) d
ON CONFLICT (date_sk) DO NOTHING;


-- ============================================================
-- 3) dim_product  (Slowly Changing Dimension - Type 1)
-- Purpose:
--   Stores the current (latest) attributes of each product.
-- Characteristics:
--   - Natural key: product_number
--   - Surrogate key: product_sk
--   - No history tracking (Type 1 overwrite)
-- Source:
--   staging.product_raw (append-only, historical)
-- Method:
--   1) Select only the latest record per product_number
--      using DISTINCT ON + ingested_at DESC
--   2) UPSERT into dim_product
--      - INSERT new products
--      - UPDATE existing products if attributes change
-- Data cleansing:
--   - Trim text fields and convert empty strings to NULL
--   - Normalize boolean flags across languages
--   - Clean currency symbols and cast prices to numeric
-- ============================================================
INSERT INTO warehouse.dim_product AS d (
  product_number,
  product_name,
  category1,
  category2,
  category3,
  season,
  discontinued,
  bestseller_abas,
  kf,
  uvp_de,
  uvp_eu,
  franchise_price
)
SELECT
  NULLIF(trim(product_number), '')              AS product_number,
  NULLIF(trim(product_name), '')                AS product_name,
  NULLIF(trim(classification1), '')             AS category1,
  NULLIF(trim(classification2), '')             AS category2,
  NULLIF(trim(classification3), '')             AS category3,
  NULLIF(trim(season), '')                      AS season,
  -- Normalize discontinued flag
  CASE
    WHEN lower(trim(discontinued)) IN ('@','ja','yes','y','true','1') THEN TRUE
    WHEN lower(trim(discontinued)) IN ('active','nein','no','n','false','0') THEN FALSE
    ELSE NULL
  END                                           AS discontinued,
  -- Normalize bestseller flag
  CASE
    WHEN lower(trim(bestseller_abas)) IN ('yes','ja','y','true','1') THEN TRUE
    WHEN lower(trim(bestseller_abas)) IN ('no','nein','n','false','0',' ') THEN FALSE
    ELSE NULL
  END                                           AS bestseller_abas,
  -- Normalize kf flag
  CASE
    WHEN lower(trim(kf)) IN ('ja','yes','y','true','1') THEN TRUE
    WHEN lower(trim(kf)) IN ('nein','no','n','false','0') THEN FALSE
    ELSE NULL
  END                                           AS kf,
  -- Clean currency symbols and cast to numeric
  NULLIF(trim(replace(replace(uvp_de,'€',''),',','')), '-')::numeric          AS uvp_de,
  NULLIF(trim(replace(replace(uvp_eu,'€',''),',','')), '-')::numeric          AS uvp_eu,
  NULLIF(trim(replace(replace(franchise_price,'€',''),',','')), '-')::numeric AS franchise_price
FROM (
  -- Select the most recently ingested record per product_number
  SELECT DISTINCT ON (product_number) *
  FROM staging.product_raw
  WHERE product_number IS NOT NULL
    AND trim(product_number) <> ''
  ORDER BY product_number, load_run_ts DESC
) p
ON CONFLICT (product_number) DO UPDATE
SET
  product_name     = EXCLUDED.product_name,
  category1        = EXCLUDED.category1,
  category2        = EXCLUDED.category2,
  category3        = EXCLUDED.category3,
  season           = EXCLUDED.season,
  discontinued     = EXCLUDED.discontinued,
  bestseller_abas  = EXCLUDED.bestseller_abas,
  kf               = EXCLUDED.kf,
  uvp_de           = EXCLUDED.uvp_de,
  uvp_eu           = EXCLUDED.uvp_eu,
  franchise_price  = EXCLUDED.franchise_price,
  last_updated_at  = now();

WITH v(raw_status, status_std) AS (
  VALUES
  -- ✅ NEW / Unconfirmed
  ('Ny (ubekræftet)', 'NEW'),
  ('Ny (ubekreftet)', 'NEW'),
  ('1. Nuevo (sin confirmar)', 'NEW'),
  ('Nuevo (sin confirmar)', 'NEW'),
  ('Novo', 'NEW'),
  ('Naujas (nepatvirtintas)', 'NEW'),
  ('Új (nem visszaigazolt)', 'NEW'),
  ('Nuovo (non confermato)', 'NEW'),
  -- ✅ CONFIRMED
  ('Ordre bekreftet', 'CONFIRMED'),
  ('2. Confirmado', 'CONFIRMED'),
  ('2. Confirmata', 'CONFIRMED'),
  ('Ordine confermato (in lavorazione)', 'CONFIRMED'),
  ('Tilaus vahvistettu', 'CONFIRMED'),
  ('Vahvistettu alustavalla toimituspäivällä', 'CONFIRMED'),
  ('Bekräftad med preliminärt sändningsdatum', 'CONFIRMED'),
  ('Novo potvrdjeno (čeka se realizacija)', 'CONFIRMED'),
  ('Nuevo confirmado (pendiente de finalizar)', 'CONFIRMED'),
  ('Patvirtintas, vykdomas', 'CONFIRMED'),
  -- ✅ PROCESSING
  ('Under behandling', 'PROCESSING'),
  ('I prosess', 'PROCESSING'),
  ('Ð’ Ð¿Ñ€Ð¾Ñ†ÐµÑ', 'PROCESSING'),
  ('Folyamatban', 'PROCESSING'),
  ('In corso', 'PROCESSING'),
  ('Merce in preparazione', 'PROCESSING'),
  ('En proceso', 'PROCESSING'),
  ('En proceso - tienda', 'PROCESSING'),
  ('In progress (Change stock internal)', 'PROCESSING'),
  ('W trakcie realizacji (ERP)', 'PROCESSING'),
  ('Packas', 'PROCESSING'),
  ('Käsittelyssä', 'PROCESSING'),
  ('Ordine completato', 'PROCESSING'),
  -- ✅ READY TO SHIP / PICKUP
  ('Listo para ser enviado', 'READY_TO_SHIP'),
  ('4. Listo para enviar', 'READY_TO_SHIP'),
  ('Klar til å bli sendt', 'READY_TO_SHIP'),
  ('Klar til å bli sendt ', 'READY_TO_SHIP'),
  ('Ruošiamas išsiuntimui', 'READY_TO_SHIP'),
  ('Ruošiamas atsiėmimui', 'READY_TO_SHIP'),
  ('Spremno za dostavu/preuzimanje', 'READY_TO_SHIP'),
  ('Szállításra kész', 'READY_TO_SHIP'),
  ('Üzletben átvehető', 'READY_TO_SHIP'),
  -- ✅ SHIPPED
  ('5. Enviado', 'SHIPPED'),
  ('ordine spedito parzialmente', 'SHIPPED'),
  -- ✅ COMPLETED
  ('Bestillingen er fullført', 'COMPLETED'),
  ('Ordre afsluttet', 'COMPLETED'),
  ('Ordre afsluttet ', 'COMPLETED'),
  ('Ordre avsluttet', 'COMPLETED'),
  ('Beställning slutförd', 'COMPLETED'),
  ('Pedido completado', 'COMPLETED'),
  ('Orden completada', 'COMPLETED'),
  ('Užsakymas įvykdytas', 'COMPLETED'),
  ('Porudžbina realizovana', 'COMPLETED'),
  ('Narudžba zaključena', 'COMPLETED'),
  ('Narudžba zaključena ', 'COMPLETED'),
  ('Zamówienie zrealizowane      ', 'COMPLETED'),
  ('Zamówienie zakończone ', 'COMPLETED'),
  ('Megrendelés befejezve', 'COMPLETED'),
  ('TILAUS VALMIS', 'COMPLETED'),
  ('5. Livrata', 'COMPLETED'),
  -- ✅ CANCELLED
  ('6. Pedido cancelado', 'CANCELLED'),
  ('Ordine cancellato', 'CANCELLED'),
  ('ordine annullato', 'CANCELLED'),
  ('Anulowane - przelew zrealizowany', 'CANCELLED'),
  ('Törölt rendelés – dupla rendelés', 'CANCELLED'),
  ('Zamówienie anulowane- zdublowane  ', 'CANCELLED'),
  ('Zamówienie anulowane- zdublowane', 'CANCELLED'),
  -- ✅ RETURN / REFUND
  ('7. Retur', 'RETURN'),
  ('7. Devolución', 'RETURN'),
  ('Devolución', 'RETURN'),
  ('Zwrot ', 'RETURN'),
  ('Zwrot', 'RETURN'),
  ('Grąžinimas', 'RETURN'),
  ('PALAUTUS', 'RETURN'),
  ('Ritorno', 'RETURN'),
  ('Retur - Mottatt lager', 'RETURN'),
  ('Zwrot przyjęty na magazynie   ', 'RETURN'),
  ('Zwrot brak przyjęcia towaru  ', 'RETURN'),
  ('Zwrot - przekazane do działu księgowości', 'RETURN'),
  ('Zwrot płatność  przekazana do księgowości  ', 'RETURN'),
  ('Zwrot częściowy płatność przekazana do księgowości', 'RETURN'),
  ('Retur - Betaling tilbakeført konto', 'RETURN'),
  -- ✅ COMPLAINT
  ('REKLAMAATIO', 'COMPLAINT'),
  ('Reclamo', 'COMPLAINT'),
  ('Reklamacija', 'COMPLAINT'),  -- <-- اینجا تصمیم گرفتیم Complaint باشه
  -- ✅ FRAUD
  ('Fraud', 'FRAUD'),
 -- ✅ UNKNOWN / N/A
  ('N/A', 'UNKNOWN')
),
dedup AS (
  SELECT DISTINCT ON (raw_status)
    raw_status,
    status_std
  FROM v
  ORDER BY
    raw_status,
    CASE status_std
      WHEN 'FRAUD' THEN 1
      WHEN 'CANCELLED' THEN 2
      WHEN 'COMPLAINT' THEN 3
      WHEN 'RETURN' THEN 4
      WHEN 'SHIPPED' THEN 5
      WHEN 'READY_TO_SHIP' THEN 6
      WHEN 'PROCESSING' THEN 7
      WHEN 'CONFIRMED' THEN 8
      WHEN 'NEW' THEN 9
      ELSE 99
    END
)
INSERT INTO warehouse.map_order_status (raw_status, status_std)
SELECT raw_status, status_std
FROM dedup
ON CONFLICT (raw_status) DO UPDATE
SET status_std = EXCLUDED.status_std;

-- Insert monthly FX rates manually for currencies not available in ECB/Frankfurter
-- This script generates one row per month between Jan 2024 and Dec 2025
-- for both COP (Colombian Peso) and RSD (Serbian Dinar).
-- If the row already exists (same month_sk + currency_code), it will be updated (UPSERT).

INSERT INTO warehouse.fx_rates_monthly (month_sk, currency_code, avg_rate_to_eur, obs_count, source)
SELECT 
    d::date,
    x.currency_code,
    x.avg_rate_to_eur,
    0,
    'manual_fixed'
FROM generate_series('2024-01-01'::date, '2025-12-01'::date, interval '1 month') d
CROSS JOIN (
    VALUES
        ('COP', 0.00023),
        ('RSD', 0.00855)
) AS x(currency_code, avg_rate_to_eur)
ON CONFLICT (month_sk, currency_code)
DO UPDATE SET
avg_rate_to_eur = EXCLUDED.avg_rate_to_eur,
obs_count = EXCLUDED.obs_count,
source = EXCLUDED.source;

select * from warehouse.fx_rates_monthly frm where currency_code = 'RSD';

