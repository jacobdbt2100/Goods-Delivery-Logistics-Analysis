-- Add a surrogate key to keep all rows uniquely identifiable
SELECT 
  ROW_NUMBER() OVER (ORDER BY usage_id) AS surrogate_id,  -- unique ID for each row
  usage_id,
  subscription_id,
  usage_date,
  feature_name,
  usage_count,
  usage_duration_secs,
  error_count
FROM {{ ref('stg_feature_usage') }}
