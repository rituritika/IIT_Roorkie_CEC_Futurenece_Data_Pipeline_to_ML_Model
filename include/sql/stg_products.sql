CREATE OR REPLACE TABLE stg_products AS
SELECT * EXCLUDE(rn) FROM (
 SELECT TRIM(product_id) product_id, TRIM(product_name) product_name,
   COALESCE(NULLIF(TRIM(category),''),'Uncategorized') category,
   TRIM(sub_category) sub_category, TRIM(brand) brand,
   TRY_CAST(mrp AS DOUBLE) mrp, TRY_CAST(cost_price AS DOUBLE) cost_price,
   TRIM(unit) unit, TRY_CAST(is_perishable AS BOOLEAN) is_perishable,
   TRY_CAST(launch_date AS DATE) launch_date,
   ROW_NUMBER() OVER(PARTITION BY TRIM(product_id) ORDER BY _loaded_at DESC) rn
 FROM raw_products
) WHERE rn=1;
