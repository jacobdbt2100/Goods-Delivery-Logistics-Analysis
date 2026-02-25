-- trial_to_paid_conversion
WITH trial_subs AS (
    SELECT
        account_id,
        MIN(start_date) AS trial_start_date
    FROM {{ ref('fct_subscriptions') }}
    WHERE is_trial = TRUE
    GROUP BY account_id
),
paid_after_trial AS (
    SELECT
        s.account_id,
        MIN(s.start_date) AS first_paid_date
    FROM {{ ref('fct_subscriptions') }} s
    JOIN trial_subs t
      ON s.account_id = t.account_id
     AND s.is_trial = FALSE --# "where s.is_trial = FALSE" also works
     AND s.start_date > t.trial_start_date
    GROUP BY s.account_id
)
SELECT
	DATE_TRUNC('month', t.trial_start_date)::date AS month_start,
    COUNT(t.account_id) AS trial_signups,
    COUNT(p.account_id) AS trial_converted,
    ROUND(
    	COUNT(p.account_id) * 100.0 / NULLIF(COUNT(t.account_id), 0), 2
	) AS conversion_rate_pct
FROM trial_subs t
LEFT JOIN paid_after_trial p
  ON t.account_id = p.account_id
GROUP BY 1
ORDER BY 1
