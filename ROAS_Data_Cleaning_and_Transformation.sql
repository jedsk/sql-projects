/* 
Summary:
Anonymized ROAS Analysis - Cleaning Jobs, Calls, and Forms Data

This script processes data from various sources to create clean and structured output tables: 
- 'anonymized_roas_jobs_cleaned'
- 'anonymized_calls_cleaned'
- 'anonymized_forms_cleaned'

Before Running Query:
Ensure the new raw jobs table is added with the appropriate naming convention.
*/

BEGIN
  /* 
  Cleaning and Transforming Completed Jobs Data
  This section stores the cleaned data in 'anonymized_roas_jobs_cleaned' table.
  */
  CREATE OR REPLACE TABLE `your_project.your_dataset.anonymized_roas_jobs_cleaned` AS 
  WITH
    -- Replace with new raw Jobs table
    stacked_jobs AS (
      SELECT * FROM `your_project.your_dataset.your_raw_jobs_table-*`
    )
  SELECT 
    -- Parsing and cleaning various fields from the jobs data
    PARSE_DATE('%m/%d/%Y', a.completion_date) AS completion_date,
    a.customer_id, 
    INITCAP(a.customer_name_string) AS customer_name, 
    a.customer_phone_string AS customer_phone, 
    LOWER(a.customer_email_string) AS customer_email, 
    a.customer_address, 
    a.customer_zip, 
    a.customer_type, 
    a.business_unit, 
    a.job_id, 
    -- Removing currency symbols and converting to numeric
    CAST(REGEXP_REPLACE(a.jobs_total_rev, r'[\$,]', '') AS NUMERIC) AS jobs_total_revenue,
    b.campaign
  FROM stacked_jobs a
  LEFT JOIN
    -- Joining with business units table for additional data
    `your_project.your_dataset.your_business_units_table` b
  ON
    a.business_unit = b.business_unit
  -- Filtering out irrelevant data
  WHERE a.completion_date IS NOT NULL
  AND LOWER(a.business_unit) NOT LIKE '%administrative%'
  ORDER BY 1;

  /* 
  Cleaning and Transforming Calls Data
  This section stores the cleaned data in 'anonymized_calls_cleaned' table.
  */
  CREATE OR REPLACE TABLE `your_project.your_dataset.anonymized_calls_cleaned` AS
  WITH
    cleaned_calls AS (
      SELECT
        -- Extracting time and date from datetime field
        TIME(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', call_datetime)) AS call_time,
        DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', call_datetime)) AS call_date,
        -- Cleaning and formatting customer name
        INITCAP(
          CASE
            WHEN REGEXP_CONTAINS(customer_name, r',') THEN CONCAT(TRIM(SPLIT(customer_name, ',')[SAFE_OFFSET(1)]), ' ', TRIM(SPLIT(customer_name, ',')[SAFE_OFFSET(0)]))
            ELSE customer_name
          END
        ) AS customer_name,
        -- Cleaning phone numbers
        REGEXP_REPLACE(customer_phone, r'[^0-9,]', '') AS customer_phone,
        REGEXP_REPLACE(tracking_phone, r'[^0-9,]', '') AS tracking_number,
        tracking_name,
        tracking_source,
        utm_source,
        utm_medium,
        utm_term,
        gclid
      FROM `your_project.your_dataset.your_calls_table`
    )
  SELECT
    -- Selecting and joining fields for final output
    a.call_time,
    a.call_date,
    a.customer_name,
    a.customer_phone,
    a.tracking_number,
    a.tracking_name,
    a.tracking_source as call_source,
    a.utm_source,
    a.utm_medium as call_medium,
    -- Commented out: b.source,
    a.utm_term,
    b.geography,  
    b.media,
    b.placement,
    b.source_type,
    b.platform, 
    b.channel, 
    a.gclid
  FROM
    cleaned_calls a
  LEFT JOIN
    -- Joining with mapping table for enriched data
    `your_project.your_dataset.your_call_mapping_table` b
  ON
    a.tracking_number = REGEXP_REPLACE(b.number_dialed, r'[^0-9,]', '') OR a.tracking_source = b.callrail_campaign
  -- Filtering out data beyond the current month
  WHERE a.call_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
  ORDER BY 2;

  /* 
  Cleaning and Transforming Forms Data
  This section stores the cleaned data in 'anonymized_forms_cleaned' table.
  */
  CREATE OR REPLACE TABLE `your_project.your_dataset.anonymized_forms_cleaned` AS
  WITH
    cleaned_forms AS (
      SELECT
        -- Parsing form submission datetime
        TIME(PARSE_DATETIME('%m/%d/%y %l:%M %p', form_datetime)) AS time,
        DATE(PARSE_DATETIME('%m/%d/%y %l:%M %p', form_datetime)) AS date,
        INITCAP(customer_name) AS customer_name,
        REGEXP_REPLACE(customer_phone, r'[^0-9,]', '') AS customer_phone,
        LOWER(customer_email) AS customer_email,
        utm_source,
        utm_term,
        gclid
      FROM `your_project.your_dataset.your_forms_table` a 
    )
  SELECT
    -- Combining and formatting final output fields
    a.time,
    a.date,
    a.customer_name,
    a.customer_phone,
    a.customer_email,
    a.utm_term,
    b.source,
    b.source_type,
    b.platform,
    b.channel,
    b.media,
    b.placement,
    a.gclid
  FROM
    cleaned_forms a
  LEFT JOIN
    -- Joining with mapping table for additional insights
    `your_project.your_dataset.your_form_mapping_table` b
  ON
    LOWER(a.utm_source) = LOWER(b.source)
  -- Excluding future data
  WHERE a.date IS NOT NULL
  AND a.date < DATE_TRUNC(CURRENT_DATE(), MONTH)
  ORDER BY 2;

END;
