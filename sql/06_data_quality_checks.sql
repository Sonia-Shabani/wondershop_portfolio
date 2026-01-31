/* ============================================================
   08_data_quality_checks.sql
   Data Quality Checks for WonderShop DWH (PostgreSQL)

   Covers:
   - Staging raw sanity checks
   - Warehouse dimension integrity
   - Sales_current quality + mapping coverage
   - Stock_snapshot snapshot integrity
   - FX coverage checks
   ============================================================ */

-- ------------------------------------------------------------
-- 0) Quick row counts (staging + warehouse)
-- ------------------------------------------------------------
SELECT 'staging.order_raw'   AS table_name, count(*) AS row_count FROM staging.order_raw
UNION ALL
SELECT 'staging.product_raw' AS table_name, count(*) AS row_count FROM staging.product_raw
UNION ALL
SELECT 'staging.stock_raw'   AS table_name, count(*) AS row_count FROM staging.stock_raw
UNION ALL
SELECT 'warehouse.dim_product'    AS table_name, count(*) AS row_count FROM warehouse.dim_product
UNION ALL
SELECT 'warehouse.dim_country'    AS table_name, count(*) AS row_count FROM warehouse.dim_country
UNION ALL
SELECT 'warehouse.dim_date'       AS table_name, count(*) AS row_count FROM warehouse.dim_date
UNION ALL
SELECT 'warehouse.sales_current'  AS table_name, count(*) AS row_count FROM warehouse.sales_current
UNION ALL
SELECT 'warehouse.stock_snapshot' AS table_name, count(*) AS row_count FROM warehouse.stock_snapshot
UNION ALL
SELECT 'warehouse.fx_rates_monthly' AS table_name, count(*) AS row_count FROM warehouse.fx_rates_monthly;

-- ------------------------------------------------------------
-- 1) STAGING: Sales raw basic validity
-- ------------------------------------------------------------

-- 1.1 Missing essential keys
SELECT
  'staging.order_raw: missing keys' AS check_name,
  count(*) AS bad_rows
FROM staging.order_raw
WHERE NULLIF(trim(country), '') IS NULL
   OR NULLIF(trim(order_id), '') IS NULL
   OR NULLIF(trim(product_number), '') IS NULL;

-- 1.2 Delivery Fee rows present (should be excluded in warehouse loads)
SELECT
  'staging.order_raw: delivery fee rows' AS check_name,
  count(*) AS rows
FROM staging.order_raw
WHERE product_name = 'Delivery Fee';

-- 1.3 created_at castability (invalid timestamps)
SELECT
  'staging.order_raw: invalid created_at timestamp' AS check_name,
  count(*) AS bad_rows
FROM staging.order_raw
WHERE NULLIF(trim(created_at), '') IS NOT NULL
  AND to_timestamp(NULLIF(trim(created_at), ''), 'YYYY-MM-DD"T"HH24:MI:SS') IS NULL
  -- NOTE: If your created_at format is not ISO-like, replace with correct parsing.
  ;

-- If created_at is ISO and PostgreSQL can parse it directly, use this instead:
-- SELECT count(*) FROM staging.order_raw
-- WHERE NULLIF(trim(created_at),'') IS NOT NULL
--   AND (NULLIF(trim(created_at),'')::timestamp IS NULL);

-- 1.4 quantity and unit_price numeric castability (non-numeric content)
SELECT
  'staging.order_raw: quantity not numeric' AS check_name,
  count(*) AS bad_rows
FROM staging.order_raw
WHERE NULLIF(trim(quantity), '') IS NOT NULL
  AND trim(quantity) !~ '^-?\d+(\.\d+)?$';

SELECT
  'staging.order_raw: unit_price not numeric' AS check_name,
  count(*) AS bad_rows
FROM staging.order_raw
WHERE NULLIF(trim(unit_price), '') IS NOT NULL
  AND trim(unit_price) !~ '^-?\d+(\.\d+)?$';

-- 1.5 Duplicate “business keys” inside staging (informational)
-- This is OK in append-only staging, but good to know volume of duplicates.
SELECT
  'staging.order_raw: duplicate (country, order_id, product_number)' AS check_name,
  count(*) AS duplicate_groups
FROM (
  SELECT country, order_id, product_number
  FROM staging.order_raw
  WHERE NULLIF(trim(country),'') IS NOT NULL
    AND NULLIF(trim(order_id),'') IS NOT NULL
    AND NULLIF(trim(product_number),'') IS NOT NULL
  GROUP BY 1,2,3
  HAVING count(*) > 1
) d;

-- ------------------------------------------------------------
-- 2) STAGING: Product raw basic validity
-- ------------------------------------------------------------

-- 2.1 Missing product_number
SELECT
  'staging.product_raw: missing product_number' AS check_name,
  count(*) AS bad_rows
FROM staging.product_raw
WHERE NULLIF(trim(product_number), '') IS NULL;

-- 2.2 Duplicates by product_number in staging (expected if history exists)
SELECT
  'staging.product_raw: product_number duplicate groups' AS check_name,
  count(*) AS duplicate_groups
FROM (
  SELECT product_number
  FROM staging.product_raw
  WHERE NULLIF(trim(product_number),'') IS NOT NULL
  GROUP BY 1
  HAVING count(*) > 1
) d;

-- ------------------------------------------------------------
-- 3) WAREHOUSE: Dimension integrity checks
-- ------------------------------------------------------------

-- 3.1 dim_product uniqueness (should be enforced by UNIQUE constraint)
SELECT
  'warehouse.dim_product: duplicate product_number (should be 0)' AS check_name,
  count(*) AS duplicate_groups
FROM (
  SELECT product_number
  FROM warehouse.dim_product
  GROUP BY 1
  HAVING count(*) > 1
) d;

-- 3.2 dim_country coverage for sales_current country_code
SELECT
  'warehouse.sales_current: country_code not in dim_country' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current sc
LEFT JOIN warehouse.dim_country dc
  ON sc.country_code = dc.country_code
WHERE dc.country_code IS NULL;

-- ------------------------------------------------------------
-- 4) WAREHOUSE: sales_current quality checks
-- ------------------------------------------------------------

-- 4.1 sales_current primary key duplicates (should be 0)
SELECT
  'warehouse.sales_current: duplicate business_key_hash (should be 0)' AS check_name,
  count(*) AS duplicate_groups
FROM (
  SELECT business_key_hash
  FROM warehouse.sales_current
  GROUP BY 1
  HAVING count(*) > 1
) d;

-- 4.2 Missing core fields
SELECT
  'warehouse.sales_current: missing required fields' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current
WHERE NULLIF(trim(country_code),'') IS NULL
   OR NULLIF(trim(order_id),'') IS NULL
   OR NULLIF(trim(product_number),'') IS NULL;

-- 4.3 created_at missing / null
SELECT
  'warehouse.sales_current: created_at is NULL' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current
WHERE created_at IS NULL;

-- 4.4 Negative or zero quantity (depends on business rules; here flagged)
SELECT
  'warehouse.sales_current: quantity <= 0' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current
WHERE quantity IS NOT NULL
  AND quantity <= 0;

-- 4.5 Negative unit price
SELECT
  'warehouse.sales_current: unit_price < 0' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current
WHERE unit_price IS NOT NULL
  AND unit_price < 0;

-- 4.6 Unknown / unmapped order statuses
SELECT
  'warehouse.sales_current: raw_status not mapped' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current
WHERE NULLIF(trim(raw_status),'') IS NOT NULL
  AND (status_std IS NULL OR trim(status_std) = '');

-- Show top unmapped statuses (for fixing map_order_status)
SELECT
  'TOP unmapped statuses' AS section,
  raw_status,
  count(*) AS cnt
FROM warehouse.sales_current
WHERE NULLIF(trim(raw_status),'') IS NOT NULL
  AND (status_std IS NULL OR trim(status_std) = '')
GROUP BY raw_status
ORDER BY cnt DESC
LIMIT 20;

-- 4.7 Product coverage: sales_current product_number not in dim_product
SELECT
  'warehouse.sales_current: product_number not in dim_product' AS check_name,
  count(*) AS bad_rows
FROM warehouse.sales_current sc
LEFT JOIN warehouse.dim_product dp
  ON sc.product_number = dp.product_number
WHERE dp.product_number IS NULL;

-- Show top missing product_numbers
SELECT
  'TOP missing product_numbers' AS section,
  sc.product_number,
  count(*) AS cnt
FROM warehouse.sales_current sc
LEFT JOIN warehouse.dim_product dp
  ON sc.product_number = dp.product_number
WHERE dp.product_number IS NULL
GROUP BY sc.product_number
ORDER BY cnt DESC
LIMIT 20;

-- ------------------------------------------------------------
-- 5) WAREHOUSE: stock_snapshot quality checks
-- ------------------------------------------------------------

-- 5.1 Duplicate PK groups (should be 0)
SELECT
  'warehouse.stock_snapshot: duplicate (snapshot_month, country_code, product_number)' AS check_name,
  count(*) AS duplicate_groups
FROM (
  SELECT snapshot_month, country_code, product_number
  FROM warehouse.stock_snapshot
  GROUP BY 1,2,3
  HAVING count(*) > 1
) d;

-- 5.2 Missing product_number
SELECT
  'warehouse.stock_snapshot: missing product_number' AS check_name,
  count(*) AS bad_rows
FROM warehouse.stock_snapshot
WHERE NULLIF(trim(product_number),'') IS NULL;

-- 5.3 Negative stock
SELECT
  'warehouse.stock_snapshot: negative stock' AS check_name,
  count(*) AS bad_rows
FROM warehouse.stock_snapshot
WHERE sellable_stock IS NOT NULL
  AND sellable_stock < 0;

-- 5.4 Product coverage for stock
SELECT
  'warehouse.stock_snapshot: product_number not in dim_product' AS check_name,
  count(*) AS bad_rows
FROM warehouse.stock_snapshot ss
LEFT JOIN warehouse.dim_product dp
  ON ss.product_number = dp.product_number
WHERE dp.product_number IS NULL;

-- ------------------------------------------------------------
-- 6) FX coverage checks (monthly)
-- ------------------------------------------------------------

-- 6.1 FX rates missing for country currencies for recent months (last 3 months)
-- Requirement:
--   For each month and each currency in dim_country, an fx rate should exist.
WITH months AS (
  SELECT date_trunc('month', (current_date - (n || ' month')::interval))::date AS month_sk
  FROM generate_series(0, 2) n
),
needed AS (
  SELECT m.month_sk, c.currency_code
  FROM months m
  CROSS JOIN (SELECT DISTINCT currency_code FROM warehouse.dim_country) c
),
missing AS (
  SELECT n.month_sk, n.currency_code
  FROM needed n
  LEFT JOIN warehouse.fx_rates_monthly fx
    ON fx.month_sk = n.month_sk
   AND fx.currency_code = n.currency_code
  WHERE fx.currency_code IS NULL
)
SELECT
  'warehouse.fx_rates_monthly: missing rates (last 3 months)' AS check_name,
  count(*) AS missing_pairs
FROM missing;

-- List missing FX pairs (debug)
WITH months AS (
  SELECT date_trunc('month', (current_date - (n || ' month')::interval))::date AS month_sk
  FROM generate_series(0, 2) n
),
needed AS (
  SELECT m.month_sk, c.currency_code
  FROM months m
  CROSS JOIN (SELECT DISTINCT currency_code FROM warehouse.dim_country) c
),
missing AS (
  SELECT n.month_sk, n.currency_code
  FROM needed n
  LEFT JOIN warehouse.fx_rates_monthly fx
    ON fx.month_sk = n.month_sk
   AND fx.currency_code = n.currency_code
  WHERE fx.currency_code IS NULL
)
SELECT
  'MISSING_FX' AS section,
  month_sk,
  currency_code
FROM missing
ORDER BY month_sk DESC, currency_code;

-- ------------------------------------------------------------
-- 7) Final “OK/Fail” summary style (optional quick view)
-- ------------------------------------------------------------
-- You can interpret any non-zero "bad_rows"/"duplicate_groups" as issues to fix.