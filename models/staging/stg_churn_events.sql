SELECT
  churn_event_id::TEXT               AS churn_event_id,
  account_id::TEXT                   AS account_id,
  churn_date::DATE                   AS churn_date,
  reason_code::TEXT                  AS reason_code,
  --refund_amount_usd::NUMERIC(12,2)   AS refund_amount_usd,
  preceding_upgrade_flag::BOOLEAN    AS preceding_upgrade_flag,
  preceding_downgrade_flag::BOOLEAN  AS preceding_downgrade_flag,
  is_reactivation::BOOLEAN           AS is_reactivation,
  --feedback_text::TEXT                AS feedback_text,
  CURRENT_TIMESTAMP                  AS ingested_at
FROM {{ source('SaaS_Analytics', 'churn_events') }}
