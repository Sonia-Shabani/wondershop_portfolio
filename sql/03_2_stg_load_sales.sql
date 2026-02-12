-- ============================
-- 03_2_load_staging_sales.sql
-- Load 1 file into staging.order_raw (append-only)
-- ============================

\set country 'BG'
\set file_path 'C:/Users/sshab/wondershop_data/sales/BG/2024_2025/<PUT_FILE_NAME_HERE>.csv'
\set source_file '<PUT_FILE_NAME_HERE>.csv'

-- 1) temp load table (matches CSV columns)
DROP TABLE IF EXISTS order_raw_load;

create TEMP TABLE order_raw_load (
  id                   text,
  ordernumber          text,
  overallstatusname    text,
  channelname          text,
  productnumber        text,
  productname          text,
  quantity             text,
  unitprice            text,
  promiseddeliveryat   text,
  manufacturer         text,
  createdat            text
);

-- 2) copy csv -> temp table
\copy order_raw_load (
  id, ordernumber, overallstatusname, channelname, productnumber,
  productname, quantity, unitprice, promiseddeliveryat, manufacturer, createdat
)
FROM :'file_path'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 3) insert into raw staging + add country + source_file
INSERT INTO staging.order_raw (
  country,
  order_id,
  order_number,
  overall_status_name,
  channel_name,
  product_number,
  product_name,
  quantity,
  unit_price,
  promised_delivery_at,
  manufacturer,
  created_at,
  source_file
)
SELECT
  :'country',
  id,
  ordernumber,
  overallstatusname,
  channelname,
  productnumber,
  productname,
  quantity,
  unitprice,
  promiseddeliveryat,
  manufacturer,
  createdat,
  :'source_file'
FROM order_raw_load;

SELECT count(*) FROM staging.order_raw;


SELECT source_file, country, count(*) FROM staging.order_raw GROUP BY source_file, country;

