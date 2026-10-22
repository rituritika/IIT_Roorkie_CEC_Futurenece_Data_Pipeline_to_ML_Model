CREATE OR REPLACE TABLE stg_customers AS
SELECT * EXCLUDE (rn) FROM (
 SELECT customer_id, full_name,
   CASE WHEN email IS NULL OR TRIM(email)='' THEN NULL
        ELSE REGEXP_REPLACE(LOWER(TRIM(email)),
          '^(.{2}).*(@.*)$', '\\1***\\2') END AS email,
   CASE WHEN phone IS NULL OR TRIM(phone)='' THEN NULL
        ELSE SUBSTR(TRIM(phone),1,2)||'******'||RIGHT(TRIM(phone),2) END AS phone,
   TRIM(city) AS city, TRIM(area) AS area, TRIM(pincode) AS pincode,
   TRY_CAST(signup_date AS DATE) AS signup_date,
   UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
   UPPER(TRIM(is_active)) AS is_active,
   ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY _loaded_at DESC) rn
 FROM raw_customers
) WHERE rn=1;
