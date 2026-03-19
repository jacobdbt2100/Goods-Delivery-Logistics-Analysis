/* model: int_accounts_age_and_last_activity */

-- Calculates account age in months and derives last activity per account from feature usage

WITH max_date AS (
    -- Latest usage date
    SELECT MAX(usage_date) AS max_usage_date
    FROM {{ ref('stg_feature_usage') }}
),

usage_with_accounts AS (
    -- Map feature usage to account_id via subscriptions
    SELECT
        fu.usage_date,
        s.account_id
    FROM {{ ref('stg_feature_usage') }} fu
    LEFT JOIN {{ ref('stg_subscriptions') }} s
        ON fu.subscription_id = s.subscription_id
),

last_activity AS (
    -- Most recent activity date per account
    SELECT
        account_id,
        MAX(usage_date) AS last_active_date
    FROM usage_with_accounts
    GROUP BY account_id
)

SELECT
    a.account_id,
    a.account_name,
    a.industry,
    a.country,
    a.signup_date,
    a.referral_source,
    a.seats,

    -- Account age in months
    EXTRACT(YEAR FROM AGE(m.max_usage_date, a.signup_date)) * 12 +  -- use CURRENT_DATE in production
    EXTRACT(MONTH FROM AGE(m.max_usage_date, a.signup_date)) AS account_age_months,  -- use CURRENT_DATE in production

    la.last_active_date,

    -- Days since last activity
    CASE 
        WHEN la.last_active_date IS NOT NULL 
        THEN m.max_usage_date - la.last_active_date  -- use CURRENT_DATE in production
        ELSE NULL
    END AS days_since_last_activity

FROM {{ ref('stg_accounts') }} a
LEFT JOIN last_activity la
    ON a.account_id = la.account_id
CROSS JOIN max_date m
