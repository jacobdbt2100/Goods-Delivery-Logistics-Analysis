-- paid_subs_engagement_rate
WITH monthly_usage AS (
    SELECT
        DATE_TRUNC('month', usage_date)::date AS month_start,
        subscription_id
    FROM {{ ref('fct_feature_usage') }}
    GROUP BY 1, 2
),
monthly_active_subs AS (
    SELECT
        DATE_TRUNC('month', dd)::date AS month_start
    FROM generate_series('2023-01-01'::date, CURRENT_DATE, interval '1 month') dd
),
active_subs AS (
    SELECT
        m.month_start,
        s.subscription_id
    FROM monthly_active_subs m
    JOIN {{ ref('fct_subscriptions') }} s
      ON s.start_date <= (m.month_start + interval '1 month - 1 day')
     AND COALESCE(s.end_date, CURRENT_DATE) >= m.month_start
     AND s.is_trial = FALSE
)
SELECT
    a.month_start,
    COUNT(DISTINCT a.subscription_id) AS active_paid_subscriptions,
    COUNT(DISTINCT u.subscription_id) AS subscriptions_with_engagement,
    ROUND(
        100.0 * COUNT(DISTINCT u.subscription_id) / NULLIF(COUNT(DISTINCT a.subscription_id), 0),
        2
    ) AS engagement_rate_pct
FROM active_subs a
LEFT JOIN monthly_usage u
  ON a.month_start = u.month_start
 AND a.subscription_id = u.subscription_id
GROUP BY 1
ORDER BY 1
