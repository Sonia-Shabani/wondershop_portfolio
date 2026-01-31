INSERT INTO warehouse.sales_current AS w (
  business_key_hash,
  change_hash,
  country_code,
  order_id,
  product_number,
  order_number,
  raw_status,
  status_std,
  channel_name,
  product_name,
  manufacturer,
  created_at,
  promised_delivery_at,
  quantity,
  unit_price,
  source_file,
  last_ingested_at
)
SELECT
  -- ==========================================================
  -- Business key hash
  -- Purpose:
  --   Uniquely identifies an order line.
  --   Used as the primary key for UPSERT operations.
  -- Logic:
  --   Hash of (country + order_id + product_number)
  -- ==========================================================
  md5(
    coalesce(s.country,'') || '|' ||
    coalesce(s.order_id,'') || '|' ||
    coalesce(s.product_number,'')
  ) AS business_key_hash,

  -- ==========================================================
  -- Change detection hash
  -- Purpose:
  --   Detects whether meaningful attributes have changed.
  --   Prevents unnecessary updates when data is identical.
  -- Logic:
  --   Hash of selected business attributes.
  -- ==========================================================
  md5(
    coalesce(s.overall_status_name,'') || '|' ||
    coalesce(s.quantity,'') || '|' ||
    coalesce(s.unit_price,'') || '|' ||
    coalesce(s.promised_delivery_at,'')
  ) AS change_hash,

  -- ==========================================================
  -- Natural business attributes
  -- ==========================================================
  s.country            AS country_code,
  s.order_id,
  s.product_number,

  s.order_number,
  s.overall_status_name AS raw_status,
  m.status_std          AS status_std,
  s.channel_name,
  s.product_name,
  s.manufacturer,

  -- ==========================================================
  -- Date and numeric normalization
  -- ==========================================================
  NULLIF(s.created_at,'')::timestamp       AS created_at,
  NULLIF(s.promised_delivery_at,'')::date AS promised_delivery_at,
  NULLIF(s.quantity,'')::numeric           AS quantity,
  NULLIF(s.unit_price,'')::numeric         AS unit_price,

  -- ==========================================================
  -- Metadata / audit columns
  -- ==========================================================
  s.source_file,
  now() AS last_ingested_at

FROM staging.order_raw s

-- ==========================================================
-- Map raw order statuses to standardized status codes
-- ==========================================================
LEFT JOIN warehouse.map_order_status m
  ON trim(s.overall_status_name) = m.raw_status

WHERE
  -- Exclude non-product rows
  s.product_name <> 'Delivery Fee'

  -- Reprocess only the current and previous month
  -- This supports late-arriving updates without scanning all history
  AND NULLIF(s.created_at,'')::date >=
      (date_trunc('month', current_date) - interval '1 month')::date

ON CONFLICT (business_key_hash) DO UPDATE
SET
  -- ==========================================================
  -- Update only when the record has actually changed
  -- ==========================================================
  change_hash           = EXCLUDED.change_hash,
  order_number          = EXCLUDED.order_number,
  raw_status            = EXCLUDED.raw_status,
  status_std            = EXCLUDED.status_std,
  channel_name          = EXCLUDED.channel_name,
  product_name          = EXCLUDED.product_name,
  manufacturer          = EXCLUDED.manufacturer,
  created_at            = EXCLUDED.created_at,
  promised_delivery_at  = EXCLUDED.promised_delivery_at,
  quantity              = EXCLUDED.quantity,
  unit_price            = EXCLUDED.unit_price,
  source_file           = EXCLUDED.source_file,
  last_ingested_at      = now()

WHERE
  -- Prevent unnecessary writes when nothing changed
  warehouse.sales_current.change_hash
    IS DISTINCT FROM EXCLUDED.change_hash;