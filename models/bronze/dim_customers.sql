SELECT
  account_id,
  account_name,
  industry,
  country,
  signup_date,
  referral_source,
  plan_tier,
  seats,
  is_trial,
  churn_flag,
  current_timestamp AS ingested_at
FROM {{ source('SaaS_Analytics', 'accounts') }}
