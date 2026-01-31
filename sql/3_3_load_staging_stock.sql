-- ============================
-- 03_load_staging_stock.sql
-- Load stock file into staging.stock_raw (append-only)
-- ============================

\set country 'BG'
\set file_path 'C:/Users/sshab/wondershop_data/stock/BG/<PUT_FILE_NAME_HERE>.csv'
\set source_file '<PUT_FILE_NAME_HERE>.csv'

-- 1) temp load table
DROP TABLE IF EXISTS staging.stock_raw_load;

CREATE TABLE staging.stock_raw_load (
  productnumber       text,
  stocksellablestock  text
);

-- 2) copy csv -> temp table
\copy staging.stock_raw_load (
  productnumber,
  stocksellablestock
)
FROM :'file_path'
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
FROM staging.stock_raw_load;

SELECT count(*) FROM staging.order_raw;
SELECT count(*) FROM staging.product_raw;
SELECT count(*) FROM staging.stock_raw;

SELECT country, count(*) FROM staging.order_raw GROUP BY country;
SELECT country, count(*) FROM staging.stock_raw GROUP BY country;