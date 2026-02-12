CREATE SCHEMA IF NOT EXISTS warehouse;

-- =========================
-- DIM: PRODUCT (Type 1)
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
  product_sk        bigserial PRIMARY KEY,
  product_number    text NOT NULL UNIQUE,
  product_name      text,
  category1         text,
  category2         text,
  category3         text,
  season            text,
  discontinued      boolean,
  bestseller_abas   boolean,
  kf                boolean,
  uvp_de            numeric(18,2),
  uvp_eu            numeric(18,2),
  franchise_price   numeric(18,2),
  last_updated_at   timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- DIM: COUNTRY
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_country (
  country_sk     bigserial PRIMARY KEY,
  country_name   text NOT NULL UNIQUE,
  country_code   text NOT NULL,
  currency_code  text NOT NULL
);

-- =========================
-- DIM: DATE
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  date_sk      date PRIMARY KEY,
  year         int NOT NULL,
  quarter      int NOT NULL,
  month_number int NOT NULL,
  year_month   text NOT NULL
);

-- =========================
-- FX RATES MONTHLY
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.fx_rates_monthly (
  month_sk          date NOT NULL,
  currency_code     text NOT NULL,
  avg_rate_to_eur   numeric(18,8) NOT NULL,
  obs_count         int NOT NULL,
  source            text NOT NULL DEFAULT 'frankfurter/ecb',
  PRIMARY KEY (month_sk, currency_code)
);

-- =========================
-- ORDER STATUS MAP
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.map_order_status (
  raw_status text PRIMARY KEY,
  status_std text NOT NULL
);

-- =========================
-- SALES CURRENT (clean/current layer)
--  این جدول "آخرین نسخه" هر order line را نگه می‌دارد
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.sales (
  business_key_hash text PRIMARY KEY,          -- unique key for upsert
  change_hash       text NOT NULL,             -- detect real change
  country_code      text NOT NULL,
  order_id          text NOT NULL,
  product_number    text NOT NULL,
  order_number         text,
  raw_status           text,
  status_std           text,
  channel_name         text,
  product_name         text,
  manufacturer         text,
  created_at           timestamp,
  promised_delivery_at date,
  quantity             numeric(18,4),
  unit_price           numeric(18,4),
  source_file          text,
  last_ingested_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sales_country_created
  ON warehouse.sales (country_code, created_at);

CREATE INDEX IF NOT EXISTS ix_sales_product_number
  ON warehouse.sales (product_number);

-- =========================
-- STOCK SNAPSHOT (monthly)
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.stock (
  snapshot_month  date NOT NULL,               -- e.g. 2026-01-01
  country_code    text,
  product_number  text NOT NULL,
  sellable_stock  numeric(18,4),
  source_file     text,
  ingested_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (snapshot_month, country_code, product_number)
);