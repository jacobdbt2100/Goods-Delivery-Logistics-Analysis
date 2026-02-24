CREATE DATABASE saas_subscription_and_churn_analytics_dev;

CREATE SCHEMA source_schema_saas_dev;

CREATE TABLE source_schema_saas_dev.accounts (
    account_id         VARCHAR(50) PRIMARY KEY,
    account_name       VARCHAR(50),
    industry           VARCHAR(50),
    country            VARCHAR(50),
    signup_date        DATE,
    referral_source    VARCHAR(50),
    plan_tier          VARCHAR(50),
    seats              INT,
    is_trial           BOOLEAN,
    churn_flag         BOOLEAN
);

CREATE TABLE source_schema_saas_dev.churn_events (
    churn_event_id           VARCHAR(50) PRIMARY KEY,
    account_id              VARCHAR(50),
    churn_date               DATE,
    reason_code              VARCHAR(50),
    refund_amount_usd        DECIMAL(12, 2),
    preceding_upgrade_flag   BOOLEAN,
    preceding_downgrade_flag BOOLEAN,
    is_reactivation          BOOLEAN,
    feedback_text            TEXT
);

CREATE TABLE source_schema_saas_dev.feature_usage (
    usage_id             VARCHAR(50),
    subscription_id      VARCHAR(50),
    usage_date           DATE,
    feature_name         VARCHAR(100),
    usage_count          INT,
    usage_duration_secs  INT,
    error_count          INT,
    is_beta_feature      BOOLEAN
);

CREATE TABLE source_schema_saas_dev.subscriptions (
    subscription_id      VARCHAR(50) PRIMARY KEY,
    account_id           VARCHAR(50),
    start_date           DATE,
    end_date             DATE,
    plan_tier            VARCHAR(50),
    seats                INT,
    mrr_amount           DECIMAL(12, 2),
    arr_amount           DECIMAL(12, 2),
    is_trial             BOOLEAN,
    upgrade_flag         BOOLEAN,
    downgrade_flag       BOOLEAN,
    churn_flag           BOOLEAN,
    billing_frequency    VARCHAR(20),
    auto_renew_flag      BOOLEAN
);

CREATE TABLE source_schema_saas_dev.support_tickets (
    ticket_id                    VARCHAR(20) PRIMARY KEY,
    account_id                   VARCHAR(20),
    submitted_at                 DATE,
    closed_at                    TIMESTAMP,
    resolution_time_hours        DECIMAL(5,2),
    priority                     VARCHAR(20),
    first_response_time_minutes  INT,
    satisfaction_score           DECIMAL(5,2),
    escalation_flag              BOOLEAN
);

select * from source_schema_saas_dev.accounts limit 3;
select * from source_schema_saas_dev.churn_events limit 3;
select * from source_schema_saas_dev.feature_usage limit 3; -- usage_id key has duplicates
select * from source_schema_saas_dev.subscriptions limit 3;
select * from source_schema_saas_dev.support_tickets limit 3;
--================================================================
