-- ============================
-- 03_3_load_staging_stock.sql
-- Load stock file into staging.stock_raw (append-only)
-- ============================

\set country 'BG'
\set source_file '<PUT_FILE_NAME_HERE>.csv'

-- 1) temp load table
DROP TABLE IF EXISTS stock_raw_load;

CREATE TEMP TABLE stock_raw_load (
  productnumber       text,
  stocksellablestock  text
);

-- 2) copy csv -> temp table
\copy stock_raw_load (
  productnumber,
  stocksellablestock
)
FROM :'C:/Users/sshab/wondershop_data/stock/XX/2026/stock_XX_01_19_2026.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 3) insert into raw staging + country + source_file
INSERT INTO staging.stock_raw (
  country,
  product_number,
  sellable_stock,
  source_file
)
SELECT
  :'country',
  productnumber,
  stocksellablestock,
  :'source_file'
FROM stock_raw_load;



