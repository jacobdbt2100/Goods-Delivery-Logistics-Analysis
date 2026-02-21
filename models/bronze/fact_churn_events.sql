SELECT
  churn_event_id,
  account_id,
  churn_date,
  reason_code,
  refund_amount_usd,
  preceding_upgrade_flag,
  preceding_downgrade_flag,
  is_reactivation	feedback_text,
  current_timestamp AS ingested_at
FROM {{ source('SaaS_Analytics', 'churn_events') }}
