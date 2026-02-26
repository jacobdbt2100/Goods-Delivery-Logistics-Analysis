-- churn_reason_distribution
SELECT
    reason_code,
    COUNT(*) AS churn_events
FROM {{ref('fct_churn_events')}}
GROUP BY 1
ORDER BY churn_events DESC
