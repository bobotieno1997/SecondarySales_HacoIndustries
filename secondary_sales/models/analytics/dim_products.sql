{{ config(
    materialized='table',
    schema='analytics',
    alias='dim_products',
    tags=['staging', 'dim_products'],
    on_schema_change='fail'
) }}

WITH products AS (
  SELECT 
    * 
  FROM 
    {{ ref('products') }} p 
  ORDER BY 
    product_brand, 
    product_desc
) 
SELECT 
  ROW_NUMBER() OVER() AS product_id, 
  * 
FROM 
  products


    