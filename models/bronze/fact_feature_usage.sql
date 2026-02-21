SELECT
  usage_id, -- has duplicates
  subscription_id,
  usage_date,
  feature_name,
  usage_count,
  usage_duration_secs,
  error_count,
  is_beta_feature,
  current_timestamp AS ingested_at
FROM {{ source('SaaS_Analytics', 'feature_usage') }}
