{{ config(
    materialized='table',
    schema='analytics',
    alias='fact_stock',
    tags=['staging', 'fact_stock'],
    on_schema_change='fail'
) }}

WITH sales AS (
  SELECT 
    dc.customer_id ,
    dp.product_id,
    s."period",
    s."week",
    s.quantity,
    s.price,
    s.source_table
  FROM 
     {{ ref('stock') }} s 
  LEFT JOIN 
     {{ ref('dim_customers') }} dc ON dc.customer_code = s.customer_code
  LEFT JOIN
     {{ ref('dim_products') }} dp ON dp.product_code = s.product_code
  ORDER BY s."period",s.week
) 
SELECT 
  ROW_NUMBER() OVER() AS sales_id, 
  * 
FROM 
  sales


    