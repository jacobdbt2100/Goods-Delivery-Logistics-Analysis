-- reactivation_cycles
WITH churned AS (
    SELECT
        account_id,
        churn_date::date AS churn_date
    FROM {{ref('fct_churn_events')}}
),
reactivation AS (
    SELECT
        c.account_id,
        c.churn_date,
        MIN(s.start_date) AS reactivation_date
    FROM churned c
    LEFT JOIN {{ref('fct_subscriptions')}} s
      ON s.account_id = c.account_id
     AND s.start_date > c.churn_date
    GROUP BY 1, 2
)
SELECT
    DATE_TRUNC('month', churn_date)::date AS month_start,
    COUNT(*) AS churned_accounts,
    COUNT(CASE WHEN reactivation_date IS NOT NULL THEN 1 END) AS reactivated_accounts,
    ROUND(
        100.0 * COUNT(CASE WHEN reactivation_date IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) AS reactivation_rate_pct,
    ROUND(AVG(reactivation_date - churn_date), 2) AS avg_days_to_reactivate
FROM reactivation
GROUP BY 1
ORDER BY 1
