CREATE OR REPLACE TABLE stg_stores AS
SELECT * EXCLUDE(rn) FROM (
 SELECT TRIM(store_id) store_id, TRIM(store_name) store_name,
   UPPER(TRIM(r."zone")) AS zone, TRIM(area) area, TRIM(pincode) pincode,
   TRY_CAST(open_date AS DATE) open_date, TRIM(manager_name) manager_name,
   TRY_CAST(capacity_orders_per_day AS INTEGER) capacity_orders_per_day,
   ROW_NUMBER() OVER(PARTITION BY TRIM(store_id) ORDER BY _loaded_at DESC) rn
 FROM raw_stores r
) WHERE rn=1;
