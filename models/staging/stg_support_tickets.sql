SELECT
  ticket_id::TEXT                          AS ticket_id,
  account_id::TEXT                         AS account_id,
  submitted_at::DATE                       AS submitted_at,
  closed_at::TIMESTAMP                     AS closed_at,
  --resolution_time_hours::NUMERIC(5,2)      AS resolution_time_hours,
  priority::TEXT                           AS priority,
  first_response_time_minutes::INTEGER     AS first_response_time_minutes,
  satisfaction_score::NUMERIC(5,2)         AS satisfaction_score,
  escalation_flag::BOOLEAN                 AS escalation_flag,
  CURRENT_TIMESTAMP                        AS ingested_at
FROM {{ source('SaaS_Analytics', 'support_tickets') }}
