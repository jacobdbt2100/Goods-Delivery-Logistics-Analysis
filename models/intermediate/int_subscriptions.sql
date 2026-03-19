SELECT
  subscription_id,
  account_id,
  start_date,
  end_date,
  plan_tier,
  seats,
  mrr_amount,
  arr_amount,
  is_trial,
  upgrade_flag,
  downgrade_flag,
  billing_frequency,
  auto_renew_flag
FROM {{ ref('stg_subscriptions') }}
