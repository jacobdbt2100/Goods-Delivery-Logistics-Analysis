-- trial_to_paid_to_upgrade_funnels
WITH account_path AS (
    SELECT
        account_id,
        MAX(CASE WHEN is_trial = TRUE THEN 1 ELSE 0 END) AS had_trial,
        MAX(CASE WHEN is_trial = FALSE THEN 1 ELSE 0 END) AS had_paid,
        MAX(CASE WHEN upgrade_flag = TRUE THEN 1 ELSE 0 END) AS had_upgrade
    FROM {{ ref('fct_subscriptions') }}
    GROUP BY account_id
)
SELECT
    COUNT(*) AS total_accounts,
    SUM(had_trial) AS accounts_with_trial,
    SUM(CASE WHEN had_trial = 1 AND had_paid = 1 THEN 1 ELSE 0 END) AS trial_to_paid,
    SUM(CASE WHEN had_trial = 1 AND had_paid = 1 AND had_upgrade = 1 THEN 1 ELSE 0 END) AS trial_paid_upgrade
FROM account_path
