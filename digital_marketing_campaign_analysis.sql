"This SQL query showcases complex data extraction and manipulation from a digital marketing database. 
  It collects information about various campaigns, including details like account ID, account name, campaign, geography, platform, media, ad type, and language. 
  Further, it processes the conversion data by distinguishing between different conversion types, calculating the number of conversions and visits, and identifying the conversion source. 
  The query excludes data related to 'youtube' in the campaign name, conversion type name, and profile. 
  It demonstrates the usage of multiple SQL features including Regular Expression, String Manipulation, Conditional Logic (CASE WHEN), and JOIN operations."

SELECT a.date,
       a.profile_id AS account_id,
       a.profile AS account_name,
       an.client_name AS client,
       an.account_name AS account,
       a.campaign_id,
       TRIM(REGEXP_EXTRACT(a.campaign_name, r'\](.*?)-')) AS campaign,
       TRIM(ARRAY_REVERSE(SPLIT(a.campaign_name, " - "))[SAFE_OFFSET(0)]) AS geography,
       "Digital" AS platform,
       t.media_ppc AS media,
       "Platform PPC" AS placement,
       TRIM(REGEXP_EXTRACT(a.campaign_name, r'-(.*?)-')) AS tactic,
       t.ad_type AS ad_type,
       "English" AS language,
       a.conversion_tracker_id,
       REGEXP_EXTRACT(a.conversion_type_name, r'\[(.*)\]') AS conversion_source,
       CASE
        WHEN a.conversion_type_name IN ('First Time Call', 'Call', 'Repeat Call', 'Call Extension Completed Job Conversions') THEN 'Calls'
        ELSE TRIM(SPLIT(REGEXP_REPLACE(a.conversion_type_name, r']', '-'), '-')[SAFE_OFFSET(1)]) 
        END AS conversion_type,
       CAST(CASE 
                WHEN a.conversion_type_name LIKE '%Location Visits%' THEN 0
                ELSE a.conversions
            END AS NUMERIC) AS conversions,
       CAST(CASE 
                WHEN a.conversion_type_name LIKE '%Location Visits%' THEN 0
                ELSE a.estimated_total_conversions
            END AS NUMERIC) AS all_conversions,
       CAST(CASE 
                WHEN a.conversion_type_name LIKE '%Location Visits%' THEN a.estimated_total_conversions
                ELSE 0
            END AS NUMERIC) AS visits,
       CAST(CASE 
                WHEN a.conversion_type_name LIKE '%Location Visits%' THEN a.estimated_total_conversions
                ELSE 0
            END AS NUMERIC) AS location_visits
FROM `schema_name.table_name` a
LEFT JOIN `schema_name.mapping_tables_accounts` an ON a.profile_id = an.vendor_account_id
LEFT JOIN `schema_name.mapping_tables_tactics` t ON TRIM(REGEXP_EXTRACT(a.campaign_name, r'-(.*?)-')) = t.tactic_string and 'Platform PPC' = t.vendor
WHERE LOWER(a.campaign_name) NOT LIKE '%youtube%'
    AND LOWER(a.conversion_type_name) NOT LIKE '%youtube%'
    AND LOWER(a.profile) NOT LIKE '%youtube%'
