INSERT INTO warehouse.stock AS ss (
  snapshot_month,
  country_code,
  product_number,
  sellable_stock,
  source_file
)
SELECT
  -- ==========================================================
  -- Snapshot month
  -- Purpose:
  --   Represents the month for which the stock level is valid.
  --   All records loaded in this run are assigned to the
  --   current calendar month (YYYY-MM-01).
  -- ==========================================================
  date_trunc('month', current_date)::date AS snapshot_month,

  -- ==========================================================
  -- Country code
  -- Source:
  --   Passed through from staging.stock_raw
  -- ==========================================================
  s.country AS country_code,
  -- ==========================================================
  -- Product identifier
  -- ==========================================================
  s.product_number,
  -- ==========================================================
  -- Stock quantity
  -- Purpose:
  --   Normalize raw text values and convert to numeric.
  -- ==========================================================
  NULLIF(s.sellable_stock,'')::numeric AS sellable_stock,
  -- ==========================================================
  -- Metadata: source file name
  -- ==========================================================
  s.source_file
FROM staging.stock_raw s
WHERE
  -- ==========================================================
  -- Data quality filter
  -- Ignore rows without a product number
  -- ==========================================================
  NULLIF(s.product_number,'') IS NOT NULL
ON CONFLICT (snapshot_month, country_code, product_number) DO UPDATE
SET
  -- ==========================================================
  -- Upsert logic
  -- Purpose:
  --   If a snapshot for the same month, country, and product
  --   already exists, overwrite it with the latest values.
  -- ==========================================================
  sellable_stock = EXCLUDED.sellable_stock,
  source_file    = EXCLUDED.source_file,
  ingested_at    = now();



