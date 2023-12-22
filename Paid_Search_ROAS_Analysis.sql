/*
Query: Paid Search ROAS Analysis
Output: paid_search_roas

Prerequisite queries to run before this query:
  anonymized_jobs_calls_forms_cleaned (cleans and integrates jobs, calls, and forms data)
  anonymized_google_keywords.current_google_keywords_metrics (aggregates Google keywords metrics)

Summary:
  This script creates a comprehensive 'Paid Search' Return on Advertising Spend (ROAS) report. 
  It first combines Google keyword metrics and conversions, and then merges job data with call and form records, focusing on 'Paid Search' interactions. 
  The script uses a 30-day lookback period to join customer interaction records (calls and forms) with job completion dates based on customer phone and email matches. 
  The final output is an aggregated view of job revenue, digital platform performance, and customer interactions, essential for evaluating the effectiveness of 'Paid Search' campaigns.
*/

CREATE OR REPLACE TABLE `your_project.your_dataset.anonymized_paid_search_roas` AS

WITH google_metrics_conversions AS (
  -- Combining Google keyword metrics and conversions
  SELECT
    day,
    account_name,
    TRIM(REGEXP_EXTRACT(campaign, r'\](.*?)-')) AS campaign,
    TRIM(ARRAY_REVERSE(SPLIT(campaign, " - "))[SAFE_OFFSET(0)]) AS geography,
    search_keyword_match_type,
    search_keyword,
    cost,
    impressions,
    clicks,
    video_played_to_100,
    NULL AS conversion_source, 
    NULL AS conversion_action, 
    CAST(0 AS NUMERIC) AS conversions        
  FROM `your_project.your_dataset.current_google_keywords_metrics`
    WHERE account_name = 'Anonymized Account' AND day >= '2023-11-01'

  UNION ALL

  SELECT
    day,
    account_name,
    TRIM(REGEXP_EXTRACT(campaign, r'\](.*?)-')) AS campaign,
    TRIM(ARRAY_REVERSE(SPLIT(campaign, " - "))[SAFE_OFFSET(0)]) AS geography,
    search_keyword_match_type,
    search_keyword,  
    CAST(0 AS NUMERIC) AS cost,              
    CAST(0 AS NUMERIC) AS impressions,       
    CAST(0 AS NUMERIC) AS clicks,            
    CAST(0 AS NUMERIC) AS video_played_to_100, 
    conversion_source,
    conversion_action,
    conversions
  FROM `your_project.your_dataset.current_google_keywords_conversions`
    WHERE account_name = 'Anonymized Account' AND day >= '2023-11-01'
),

lookback_windows AS (
  -- Setting a standard 30-day lookback window
  SELECT 30 AS lookback_window
),

calls_report AS (
  -- Combining job data with 'Paid Search' call records
  SELECT
    j.*,
    l.lookback_window,
    c.utm_term,
    c.gclid 
  FROM `your_project.your_dataset.anonymized_jobs_calls_forms_cleaned` j
  CROSS JOIN lookback_windows l
  INNER JOIN `your_project.your_dataset.anonymized_calls_cleaned` c ON
    j.customer_phone = c.customer_phone
    AND c.call_date BETWEEN DATE_SUB(j.completion_date, INTERVAL l.lookback_window DAY)
    AND j.completion_date
    AND c.media = "Paid Search"
),

forms_report AS (
  -- Merging job data with 'Paid Search' form submissions
  SELECT
    j.*,
    l.lookback_window,
    f.utm_term,
    f.gclid 
  FROM `your_project.your_dataset.anonymized_jobs_calls_forms_cleaned` j
  CROSS JOIN lookback_windows l
  INNER JOIN `your_project.your_dataset.anonymized_forms_cleaned` f ON
    (j.customer_phone = f.customer_phone OR j.customer_email = f.customer_email)
    AND f.date BETWEEN DATE_SUB(j.completion_date, INTERVAL l.lookback_window DAY)
    AND j.completion_date
    AND f.media = "Paid Search"
),

stacked_calls_forms AS (
  -- Unifying the matched jobs from calls and forms data
  SELECT * FROM calls_report
  UNION ALL
  SELECT * FROM forms_report
)

-- Final aggregation and output
SELECT
  a.completion_date AS date,
  a.customer_id,
  a.customer_name,
  a.customer_phone,
  a.customer_email,
  a.customer_address,
  a.customer_zip,
  a.business_unit,
  a.job_id,
  a.utm_term AS search_keyword,
  a.gclid,
  b.campaign,
  b.geography,
  'Digital' AS platform,
  'Paid Search' AS media,
  'Google PPC' AS placement,
  'Last Touch' AS attribution_model,
  CAST(a.lookback_window AS STRING) AS lookback_window,
  IF(a.jobs_total_revenue > 0, 1, 0) AS jobs_booked,
  0 AS media_spend,
  0 AS impressions,
  0 AS clicks,
  0 AS conversions,
  0 AS all_conversions,
  a.jobs_total_revenue / COUNT(1) OVER (
    PARTITION BY a.completion_date, a.customer_id, a.customer_name, a.customer_phone,
    a.customer_email, a.customer_address, a.customer_zip, a.business_unit, a.job_id,
    a.jobs_total_revenue, a.lookback_window
  ) AS jobs_revenue
FROM stacked_calls_forms a
LEFT JOIN `your_project.your_dataset.your_business_units_table` b ON
  a.business_unit = b.business_unit

UNION ALL

SELECT
  day AS date,
  NULL AS customer_id,
  NULL AS customer_name,
  NULL AS customer_phone,
  NULL AS customer_email,
  NULL AS customer_address,
  NULL AS customer_zip,
  NULL AS business_unit,
  NULL AS job_id,
  search_keyword,
  NULL AS gclid,
  campaign,
  geography,
  'Digital' AS platform,
  'Paid Search' AS media,
  'Google PPC' AS placement,
  'Last Touch' AS attribution_model,
  '30' AS lookback_window,
  0 AS jobs_booked,
  cost AS media_spend,
  impressions,
  clicks,
  conversions,
  conversions AS all_conversions,
  0 AS jobs_revenue
FROM google_metrics_conversions

ORDER BY lookback_window, date;
