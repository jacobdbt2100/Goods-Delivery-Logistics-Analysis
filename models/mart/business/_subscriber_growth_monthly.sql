/* model: subscriber_growth_monthly */

-- New: first-ever subscription (lifetime)
-- Returning: previously subscribed and active again
-- Churned: last active subscription ended (no further activity)

WITH months AS (
    -- All months in subscription lifecycle
    SELECT DISTINCT DATE_TRUNC('month', start_date)::date AS month_start
    FROM {{ ref('stg_subscriptions') }}
    WHERE is_trial = FALSE

    UNION

    SELECT DISTINCT DATE_TRUNC('month', end_date)::date
    FROM {{ ref('stg_subscriptions') }}
    WHERE is_trial = FALSE
      AND end_date IS NOT NULL
),

first_subscription AS (
    -- First subscription date per account
    SELECT
        account_id,
        MIN(start_date) AS first_start_date
    FROM {{ ref('stg_subscriptions') }}
    WHERE is_trial = FALSE
    GROUP BY account_id
),

monthly_new AS (
    -- First-ever subscribers in each month
    SELECT
        DATE_TRUNC('month', first_start_date)::date AS month_start,
        COUNT(DISTINCT account_id) AS new_subscribers
    FROM first_subscription
    GROUP BY 1
),

last_subscription AS (
    -- Last subscription end per account
    SELECT
        account_id,
        MAX(end_date) AS last_end_date
    FROM {{ ref('stg_subscriptions') }}
    WHERE is_trial = FALSE
      AND end_date IS NOT NULL
    GROUP BY account_id
),

monthly_churn AS (
    -- Accounts whose final subscription ended in the month
    SELECT
        DATE_TRUNC('month', last_end_date)::date AS month_start,
        COUNT(DISTINCT account_id) AS churned_subscribers
    FROM last_subscription
    GROUP BY 1
),

returning_subscribers AS (
    -- Active accounts at month start (excludes new subscribers in that month)
    SELECT
        m.month_start,
        COUNT(DISTINCT s.account_id) AS returning_subscribers
    FROM months m
    JOIN {{ ref('stg_subscriptions') }} s
      ON s.start_date < m.month_start
     AND (s.end_date IS NULL OR s.end_date >= m.month_start)
     AND s.is_trial = FALSE
    GROUP BY m.month_start
)

SELECT
    m.month_start,

    COALESCE(n.new_subscribers, 0) AS new_subscribers,
    COALESCE(c.churned_subscribers, 0) AS churned_subscribers,

    -- Net growth in subscriber base
    COALESCE(n.new_subscribers, 0)
      - COALESCE(c.churned_subscribers, 0) AS net_subscriber_change,

    COALESCE(r.returning_subscribers, 0) AS returning_subscribers,

    -- Total active subscribers in the month
    COALESCE(r.returning_subscribers, 0)
      + COALESCE(n.new_subscribers, 0)
      - COALESCE(c.churned_subscribers, 0) AS total_active_subscribers

FROM months m
LEFT JOIN monthly_new n
       ON m.month_start = n.month_start
LEFT JOIN monthly_churn c
       ON m.month_start = c.month_start
LEFT JOIN returning_subscribers r
       ON m.month_start = r.month_start

ORDER BY m.month_start
