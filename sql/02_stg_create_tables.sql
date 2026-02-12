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
-- STOCK RAW (monthly snapshot input)
-- ----------------------------
CREATE TABLE IF NOT EXISTS staging.stock_raw (
  country        text,              -- e.g. BG / SE / HR (set at load time)
  product_number text,
  sellable_stock text,              -- from: stockSellableStock (raw)
  source_file    text,
  ingested_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stock_raw_product_number
  ON staging.stock_raw (product_number);

CREATE INDEX IF NOT EXISTS ix_stock_raw_country
  ON staging.stock_raw (country);

-- ----------------------------
-- Product raw
-- ----------------------------
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
  source_file      TEXT,
  load_run_ts      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_product_raw_product_number
  ON staging.product_raw (product_number);




