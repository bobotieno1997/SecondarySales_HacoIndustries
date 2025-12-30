{{ config(
    materialized='table',
    schema='datawarehouse',
    alias='customers',
    tags=['staging', 'customers'],
    on_schema_change='fail'
) }}

WITH customer_sales AS (
  SELECT 
    DISTINCT "CUSTOMER CODE" AS customer_code, 
    INITCAP("CUSTOMER NAME") AS customer_name 
  FROM 
    {{ ref( 'stg_sales_combined' )}}
), 
customer_stock AS (
  SELECT 
    DISTINCT "CUSTOMER CODE" AS customer_code, 
    INITCAP("CUSTOMER NAME") AS customer_name 
  FROM 
    {{ ref( 'stg_stock_combined' )}}
) 
SELECT 
  * 
FROM 
  customer_sales 
UNION 
SELECT 
  * 
FROM 
  customer_stock

    