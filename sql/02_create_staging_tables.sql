-- ============================================================
-- Staging schema + RAW tables (append-only)
-- Target DB: PostgreSQL
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ----------------------------
-- SALES / ORDERS RAW
-- ----------------------------
CREATE TABLE IF NOT EXISTS staging.order_raw (
  country              text,          -- e.g. BG / SE / HR (set at load time)
  order_id             text,          -- from: id
  order_number         text,          -- from: orderNumber
  overall_status_name  text,          -- from: overallStatusName
  channel_name         text,          -- from: channelName (e.g. KARE.BG)
  product_number       text,          -- from: productNumber
  product_name         text,          -- from: productName
  quantity             text,          -- keep raw; cast in warehouse
  unit_price           text,          -- keep raw; cast in warehouse
  promised_delivery_at text,          -- keep raw; cast in warehouse
  manufacturer         text,          -- from: manufacturer
  created_at           text,          -- from: createdAt (keep raw; cast in warehouse)
  source_file          text,          -- audit: filename loaded
  ingested_at          timestamptz NOT NULL DEFAULT now()
);

-- Helpful indexes for loads / filtering
CREATE INDEX IF NOT EXISTS ix_order_raw_country_createdat
  ON staging.order_raw (country, created_at);

CREATE INDEX IF NOT EXISTS ix_order_raw_order_id
  ON staging.order_raw (order_id);

CREATE INDEX IF NOT EXISTS ix_order_raw_product_number
  ON staging.order_raw (product_number);


-- ----------------------------
-- PRODUCT RAW (ABAS export)
-- ----------------------------
CREATE TABLE IF NOT EXISTS staging.product_raw (
  product_number   text,
  product_name     text,
  classification1  text,
  classification2  text,
  classification3  text,
  season           text,
  discontinued     text,             -- raw flag (cast later)
  bestseller_abas  text,             -- raw flag (cast later)
  kf               text,             -- raw flag (cast later)
  uvp_de           text,             -- raw numeric
  uvp_eu           text,             -- raw numeric
  franchise_price  text,             -- raw numeric
  source_file      text,
  ingested_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_product_raw_product_number
  ON staging.product_raw (product_number);


-- ----------------------------
-- STOCK RAW (monthly snapshot input)
-- ----------------------------
CREATE TABLE IF NOT EXISTS staging.stock_raw (
  country        text,              -- if stock is by country; otherwise can be NULL
  product_number text,
  sellable_stock text,              -- from: stockSellableStock (raw)
  source_file    text,
  ingested_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stock_raw_product_number
  ON staging.stock_raw (product_number);

CREATE INDEX IF NOT EXISTS ix_stock_raw_country
  ON staging.stock_raw (country);

-- PRODUCT raw
DROP TABLE IF EXISTS staging.product_raw;
CREATE TABLE IF NOT EXISTS staging.product_raw (
  product_number   TEXT,
  product_name     TEXT,
  classification1  TEXT,
  classification2  TEXT,
  classification3  TEXT,
  season           TEXT,
  discontinued     TEXT,
  bestseller_abas  TEXT,
  kf               TEXT,
  uvp_de           TEXT,
  uvp_eu           TEXT,
  franchise_price  TEXT,
  load_run_ts      TIMESTAMPTZ DEFAULT now()
);


SELECT country, count(*) AS count
FROM staging.order_raw t
WHERE productname <> 'Delivery Fee'
GROUP BY country
ORDER BY country;

SELECT DISTINCT overallstatusname FROM staging.order_raw;

--stock raw
DROP TABLE IF EXISTS staging.stock_raw;
CREATE TABLE IF NOT EXISTS staging.stock_raw (
product_number text, 
stock text);

-- Creating index for speed

CREATE INDEX IF NOT EXISTS ix_order_raw_productnumber ON staging.order_raw(productNumber);
CREATE INDEX IF NOT EXISTS ix_order_raw_country ON staging.order_raw(country);
CREATE INDEX IF NOT EXISTS ix_order_raw_createdat ON staging.order_raw(createdAt);
CREATE INDEX IF NOT EXISTS ix_product_raw_productnumber ON staging.product_raw(product_number);