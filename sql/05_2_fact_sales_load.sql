INSERT INTO warehouse.sales AS w (
  business_key_hash,
  change_hash,
  country_code,
  order_id,
  product_number,
  order_number,
  raw_status,
  status_std,
  channel_name,
  created_at,
  promised_delivery_at,
  quantity,
  unit_price,
  order_value,
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
  s.country           AS country_code,
  s.order_id,
  s.product_number,
  s.order_number,
  s.overall_status_name AS raw_status,
  m.status_std          AS status_std,
  s.channel_name,
  -- ==========================================================
  -- Date and numeric normalization
  -- ==========================================================
  NULLIF(NULLIF(trim(s.created_at),''),'N/A')::timestamp             AS created_at,
  NULLIF(NULLIF(trim(s.promised_delivery_at),''),'N/A')::date        AS promised_delivery_at,
  NULLIF(s.quantity,'')::numeric                                     AS quantity,
    -- Convert unit_price into EUR
  NULLIF(s.unit_price,'')::numeric * COALESCE(f.avg_rate_to_eur, 1)  AS unit_price,
  -- Convert order_value into EUR
  (NULLIF(s.quantity,'')::numeric * NULLIF(s.unit_price,'')::numeric) * COALESCE(f.avg_rate_to_eur, 1) AS order_value,
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
LEFT JOIN warehouse.dim_country c
    ON s.country = c.country_code
LEFT JOIN warehouse.fx_rates_monthly f
    ON c.currency_code = f.currency_code
	AND f.month_sk = date_trunc('month', NULLIF(NULLIF(trim(s.created_at),''),'N/A')::date)::date
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
  created_at            = EXCLUDED.created_at,
  promised_delivery_at  = EXCLUDED.promised_delivery_at,
  quantity              = EXCLUDED.quantity,
  unit_price            = EXCLUDED.unit_price,
  source_file           = EXCLUDED.source_file,
  last_ingested_at      = now()
WHERE
  -- Prevent unnecessary writes when nothing changed
  w.change_hash
    IS DISTINCT FROM EXCLUDED.change_hash;

