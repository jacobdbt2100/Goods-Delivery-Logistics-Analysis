-- churn_drivers_one_row_per_churned_account
WITH churned_accounts AS (
    SELECT
        ce.account_id,
        ce.churn_date::date AS churn_date,
        ce.reason_code,
        ce.refund_amount_usd,
        ce.preceding_upgrade_flag,
        ce.preceding_downgrade_flag,
        ce.is_reactivation
    FROM {{ref('fct_churn_events')}} ce
),
last_subscription AS (
    SELECT DISTINCT ON (s.account_id) -- Refactor to replace DISTINCT ON with ROW_NUMBER() later
        s.account_id,
        s.plan_tier,
        s.seats,
        s.mrr_amount,
        s.billing_frequency,
        s.auto_renew_flag,
        s.start_date,
        s.end_date
    FROM {{ref('fct_subscriptions')}} s
    JOIN churned_accounts c
      ON s.account_id = c.account_id
     AND s.start_date <= c.churn_date
    ORDER BY s.account_id, s.start_date DESC
),
support_90d AS (
    SELECT
        c.account_id,
        COUNT(*) AS tickets_last_90d,
        ROUND(AVG(st.first_response_time_minutes), 2) AS avg_first_response_last_90d_minutes,
        ROUND(AVG(st.resolution_time_hours), 2) AS avg_resolution_last_90d_hours
    FROM churned_accounts c
    LEFT JOIN {{ref('fct_support_tickets')}} st
      ON st.account_id = c.account_id
     AND st.submitted_at::date BETWEEN (c.churn_date - 90) AND c.churn_date
    GROUP BY 1
),
usage_90d AS (
    SELECT
        c.account_id,
        COUNT(DISTINCT fu.feature_name) AS distinct_features_used_90d,
        SUM(fu.usage_count) AS total_usage_count_90d,
        SUM(fu.error_count) AS total_errors_90d
    FROM churned_accounts c
    LEFT JOIN {{ref('fct_subscriptions')}} s
      ON s.account_id = c.account_id
    LEFT JOIN {{ref('fct_feature_usage')}} fu
      ON fu.subscription_id = s.subscription_id
     AND fu.usage_date BETWEEN (c.churn_date - 90) AND c.churn_date
    GROUP BY 1
)
SELECT
    c.account_id,
    c.churn_date,
    c.reason_code,
    c.refund_amount_usd,
    c.preceding_upgrade_flag,
    c.preceding_downgrade_flag,

    ls.plan_tier AS last_plan_tier,
    ls.seats AS last_seats,
    ls.mrr_amount AS last_mrr,
    ls.billing_frequency,
    ls.auto_renew_flag,

    sp.tickets_last_90d,
    sp.avg_first_response_last_90d_minutes,
    sp.avg_resolution_last_90d_hours,

    u.distinct_features_used_90d,
    u.total_usage_count_90d,
    u.total_errors_90d
FROM churned_accounts c
LEFT JOIN last_subscription ls ON c.account_id = ls.account_id
LEFT JOIN support_90d sp ON c.account_id = sp.account_id
LEFT JOIN usage_90d u ON c.account_id = u.account_id
ORDER BY c.churn_date DESC
