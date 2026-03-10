## End-to-End Analytics Engineering Project:
___

# Building an ELT Pipeline for SaaS Subscriber Growth and Retention

<img src="https://raw.githubusercontent.com/jacobdbt2100/SaaS-Subscription-and-Churn-Analytics/main/dbt_postgresql_elt.jpg" width="800">

## Executive Summary:

Designed and implemented a production-style ELT pipeline to transform raw SaaS data into analytics-ready models for revenue, retention, churn, reactivation, and support performance analysis.

Using dbt, PostgreSQL, and Power BI, I built a structured metrics layer that enables business stakeholders to monitor MRR, churn rate, LTV, reactivation cycles, and support load in real time.

## Business Problem:

SaaS companies often struggle with:
- Fragmented revenue data (subscriptions, upgrades, churn)
- Poor visibility into retention and reactivation behaviour
- No single source of truth for MRR and customer lifecycle metrics
- Limited connection between support activity and churn.

The goal:
Build a clean, testable, and scalable data model that supports executive-level revenue and retention decision-making.

## Methodology:

1. Data Modelling (ELT Approach)
2. Transformation Layer (dbt): Surrogate keys where necessary
3. Documented models and columns
4. Data Testing & Validation
5. Unique & not null tests
6. Relationship tests (foreign key integrity)
7. Custom logic tests (e.g., churn date > signup date)
8. CI/CD
9. Version-controlled via GitHub
10. dbt runs triggered through CI workflow
11. Automated checks before merge
12. Reporting Layer (Power BI)
13. Star schema built from metrics layer
14. Calendar table for cohort and MRR analysis
15. DAX measures for dynamic KPIs

Since `usage_id` was duplicated and had no foreign key dependencies, a surrogate primary key was introduced to enforce event-level uniqueness and preserve modelling integrity.

## Insights & Recommendations:

<img src="https://raw.githubusercontent.com/jacobdbt2100/SaaS-Subscription-and-Churn-Analytics/main/SaaS subscription and churn analysis.jpg" width="800">

### Skills:
SQL Modelling, Data Test & Validation, Documentation, Power BI Data Modelling, DAX, Data Visualization
___
