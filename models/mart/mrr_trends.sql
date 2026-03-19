-- mrr_trends
WITH months AS (
    SELECT DATE_TRUNC('month', month_date)::date AS month_start
    FROM generate_series('2023-01-01'::date, CURRENT_DATE, interval '1 month') month_date
),
sub_active_by_month AS (
    SELECT
        m.month_start,
		--s.start_date,
		--s.end_date,
        s.subscription_id,
        s.account_id,
        s.mrr_amount
    FROM months m
    JOIN {{ ref('fct_subscriptions') }} s
      ON s.start_date <= (m.month_start + interval '1 month - 1 day')
     AND COALESCE(s.end_date, CURRENT_DATE) >= m.month_start
     AND s.is_trial = FALSE
)
SELECT
    month_start,
    SUM(mrr_amount) AS total_mrr,
    COUNT(DISTINCT account_id) AS paying_accounts
FROM sub_active_by_month
GROUP BY 1
ORDER BY 1
