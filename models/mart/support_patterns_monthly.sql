-- support_patterns_monthly
SELECT
	DATE_TRUNC('month', submitted_at)::date AS month_start,
    COUNT(*) AS tickets,
    ROUND(AVG(resolution_time_hours), 2) AS avg_resolution_hours,
    ROUND(AVG(first_response_time_minutes), 2) AS avg_first_response_mins,
    ROUND(AVG(satisfaction_score), 2) AS avg_satisfaction,
    SUM(CASE WHEN escalation_flag = TRUE THEN 1 ELSE 0 END) AS escalations
FROM {{ ref('fct_support_tickets') }}
GROUP BY 1
ORDER BY 1
