/*This SQL query is designed to match completed jobs to either calls or forms based on customer phone numbers and email addresses. 
  The script first normalizes the data to ensure that phone numbers and email addresses are presented in a uniform way across different sources. 
  It then constructs a series of lookback windows for comparison purposes.

Using this lookback window, it matches completed jobs to calls or forms based on whether the phone number or email from the call or form is present in the completed jobs report, 
  and if the call or form date falls within the lookback window from the job completion date.

Finally, the query collates all matched records, removes duplicates, and sorts them by the lookback window and completion date. */


WITH
  jobs_report AS (
    SELECT *,
      SPLIT(customer_phone, ',') AS phone_numbers_array,
      SPLIT(customer_email, ',') AS email_array
    FROM `project.dataset.jobs_table`
  ),

  calls_report AS (
    SELECT 
      call_date,
      geography,
      placement,
      SPLIT(IFNULL(CONCAT(caller_phone, IF(customer_phone IS NULL, '', ','), customer_phone), caller_phone), ',') AS phone_numbers_array,
      SPLIT(customer_email, ',') AS email_array
    FROM `project.dataset.calls_table`
    WHERE media LIKE 'Paid Search'
  ),

  forms_report AS (
    SELECT 
      call_date,
      geography,
      placement,
      SPLIT(customer_phone, ',') AS phone_numbers_array,
      SPLIT(customer_email, ',') AS email_array
    FROM `project.dataset.forms_table`
    WHERE media LIKE 'Paid Search'
  ),

  lookback_windows AS (
    SELECT 30 AS lookback_window
    UNION ALL
    SELECT 14
    UNION ALL
    SELECT 7
  ),

  jobs_calls_matching AS (
    SELECT
      a.*,
      lookback_window,
      geography,
      placement
    FROM
      jobs_report AS a
    CROSS JOIN
      lookback_windows
    CROSS JOIN
      calls_report AS b
    CROSS JOIN
      UNNEST(a.phone_numbers_array) AS A_phone_number
    CROSS JOIN
      UNNEST(b.phone_numbers_array) AS B_phone_number
    WHERE
      (
        TRIM(A_phone_number) = TRIM(B_phone_number)
        OR EXISTS (
          SELECT 1 
          FROM UNNEST(a.email_array) AS A_email, UNNEST(b.email_array) AS B_email 
          WHERE TRIM(A_email) = TRIM(B_email)
          AND NOT STARTS_WITH(TRIM(A_email), 'print'))
      )
    AND B.call_date BETWEEN DATE_SUB(A.completion_date, INTERVAL lookback_window DAY) AND A.completion_date
  ),

    jobs_forms_matching AS (
    SELECT
      a.*,
      lookback_window,
      geography,
      placement
    FROM
      jobs_report AS a
    CROSS JOIN
      lookback_windows
    CROSS JOIN
      forms_report AS c
    CROSS JOIN
      UNNEST(a.phone_numbers_array) AS A_phone_number
    CROSS JOIN
      UNNEST(c.phone_numbers_array) AS C_phone_number
    WHERE
      (
        TRIM(A_phone_number) = TRIM(C_phone_number)
        OR EXISTS (
          SELECT 1 
          FROM UNNEST(a.email_array) AS A_email, UNNEST(C.email_array) AS C_email 
          WHERE TRIM(A_email) = TRIM(C_email)
          )
      )
    AND C.call_date BETWEEN DATE_SUB(A.completion_date, INTERVAL lookback_window DAY) AND A.completion_date
    )

SELECT DISTINCT
  a.completion_date,
  a.customer_id,
  INITCAP(a.customer_name) as customer_name,
  a.customer_phone,
  LOWER(a.customer_email) as customer_email,
  a.customer_address,
  a.customer_zip,
  a.business_unit,
  a.job_id,
  a.jobs_total_revenue,
  TRIM(REGEXP_EXTRACT(a.business_unit, r'-(.*?)-')) AS campaign,
  a.lookback_window,
  a.placement,
  'Paid Search' AS media,
  a.geography

FROM
  (SELECT * FROM jobs_calls_matching
  UNION ALL
  SELECT * FROM jobs_forms_matching) AS a
ORDER BY lookback_window, completion_date
