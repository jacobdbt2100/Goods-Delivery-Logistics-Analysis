SELECT
  subscription_id::TEXT           AS subscription_id,
  account_id::TEXT                AS account_id,
  start_date::DATE                AS start_date,
  end_date::DATE                  AS end_date,
  plan_tier::TEXT                 AS plan_tier,
  seats::INTEGER                  AS seats,
  mrr_amount::NUMERIC(12,2)       AS mrr_amount,
  arr_amount::NUMERIC(12,2)       AS arr_amount,
  is_trial::BOOLEAN               AS is_trial,
  upgrade_flag::BOOLEAN           AS upgrade_flag,
  downgrade_flag::BOOLEAN         AS downgrade_flag,
  --churn_flag::BOOLEAN             AS churn_flag, -- belong in churn metrics table
  billing_frequency::TEXT         AS billing_frequency,
  auto_renew_flag::BOOLEAN        AS auto_renew_flag,
  CURRENT_TIMESTAMP               AS ingested_at
FROM {{ source('SaaS_Analytics', 'subscriptions') }}
