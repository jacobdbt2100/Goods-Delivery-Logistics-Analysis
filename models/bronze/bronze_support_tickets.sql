SELECT
  ticket_id,
  account_id,
  submitted_at,
  closed_at,
  resolution_time_hours,
  priority,
  first_response_time_minutes,
  satisfaction_score,
  escalation_flag,
  current_timestamp AS ingested_at
FROM {{ source('SaaS_Analytics', 'support_tickets') }}
