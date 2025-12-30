{{ config(
    materialized='table',
    schema='datawarehouse',
    alias='products',
    tags=['staging', 'products'],
    on_schema_change='fail'
) }}

WITH product_sales AS (
  SELECT 
    DISTINCT "PRODUCT_CODE" AS product_code, 
    "DESCRIPTION" AS product_desc, 
    "VARIANT" AS product_variant, 
    "SKU" AS product_sku, 
    "BRAND" AS product_brand, 
    "CATEGORY" AS product_category, 
    "PAC" AS product_pac
  FROM 
    {{ ref( 'stg_sales_combined' )}} cs 
  ORDER BY 
    "BRAND"
), 
product_stock AS (
  SELECT 
    DISTINCT "PRODUCT_CODE" AS product_code, 
    "DESCRIPTION" AS product_desc, 
    "VARIANT" AS product_variant, 
    "SKU" AS product_sku, 
    "BRAND" AS product_brand, 
    "CATEGORY" AS product_category, 
    "PAC" AS product_pac
  FROM 
    {{ ref( 'stg_stock_combined' )}} cs 
  ORDER BY 
    "BRAND"
) 
SELECT 
  DISTINCT * 
FROM 
  product_sales 
UNION 
SELECT 
  DISTINCT * 
FROM 
  product_stock


    