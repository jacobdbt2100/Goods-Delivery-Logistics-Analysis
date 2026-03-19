SELECT
  account_id::TEXT            AS account_id,
  account_name::TEXT          AS account_name,
  industry::TEXT              AS industry,
  country::TEXT               AS country,
  signup_date::DATE           AS signup_date,
  referral_source::TEXT       AS referral_source,
  --plan_tier::TEXT             AS plan_tier, -- belong in subscription table
  seats::INTEGER              AS seats,
  --is_trial::BOOLEAN           AS is_trial, -- belong in subscription table
  --churn_flag::BOOLEAN         AS churn_flag, -- belong in churn metrics table
  CURRENT_TIMESTAMP           AS ingested_at
FROM {{ source('SaaS_Analytics', 'accounts') }}
