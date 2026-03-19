SELECT
  usage_id::TEXT                    AS usage_id,
  subscription_id::TEXT             AS subscription_id,
  usage_date::DATE                  AS usage_date,
  feature_name::TEXT                AS feature_name,
  usage_count::INTEGER              AS usage_count,
  usage_duration_secs::INTEGER      AS usage_duration_secs,
  error_count::INTEGER              AS error_count,
  --is_beta_feature::BOOLEAN          AS is_beta_feature,
  CURRENT_TIMESTAMP                 AS ingested_at
FROM {{ source('SaaS_Analytics', 'feature_usage') }}
