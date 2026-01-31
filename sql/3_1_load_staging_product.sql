-- ============================
-- 03_load_staging_product.sql
-- Load product file into staging.product_raw (append-only)
-- ============================

\set file_path 'C:/Users/sshab/wondershop_data/product/<PUT_FILE_NAME_HERE>.csv'
\set source_file '<PUT_FILE_NAME_HERE>.csv'

-- 1) temp load table
DROP TABLE IF EXISTS staging.product_raw_load;

CREATE TABLE staging.product_raw_load (
  product_number   text,
  product_name     text,
  classification1  text,
  classification2  text,
  classification3  text,
  season           text,
  discontinued     text,
  bestseller_abas  text,
  kf               text,
  uvp_de           text,
  uvp_eu           text,
  franchise_price  text
);

-- 2) copy csv -> temp table
\copy staging.product_raw_load (
  product_number,
  product_name,
  classification1,
  classification2,
  classification3,
  season,
  discontinued,
  bestseller_abas,
  kf,
  uvp_de,
  uvp_eu,
  franchise_price
)
FROM :'file_path'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 3) insert into raw staging + source_file
INSERT INTO staging.product_raw (
  product_number,
  product_name,
  classification1,
  classification2,
  classification3,
  season,
  discontinued,
  bestseller_abas,
  kf,
  uvp_de,
  uvp_eu,
  franchise_price,
  source_file
)
SELECT
  product_number,
  product_name,
  classification1,
  classification2,
  classification3,
  season,
  discontinued,
  bestseller_abas,
  kf,
  uvp_de,
  uvp_eu,
  franchise_price,
  :'source_file'
FROM staging.product_raw_load;

SELECT count(*) FROM staging.order_raw;
SELECT count(*) FROM staging.product_raw;
SELECT count(*) FROM staging.stock_raw;

SELECT country, count(*) FROM staging.order_raw GROUP BY country;
SELECT country, count(*) FROM staging.stock_raw GROUP BY country;