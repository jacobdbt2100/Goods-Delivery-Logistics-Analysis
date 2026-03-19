/* model: int_subscriptions_cohort */

-- Derives first subscription month (cohort month) per paying account

SELECT
    account_id,
    start_date AS subscription_start_date,

    -- Cohort month (end of month) based on first subscription date per account
    (
        DATE_TRUNC(
            'month',
            MIN(start_date) OVER (PARTITION BY account_id)
        )
        + INTERVAL '1 month - 1 day'
    )::DATE AS cohort_month_eom

FROM {{ ref('stg_subscriptions') }}
WHERE is_trial is FALSE
