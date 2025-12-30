{{ config(
    materialized='table',
    schema='datawarehouse',
    alias='sales',
    tags=['datawarehouse', 'sales'],
    on_schema_change='fail'
) }}

WITH unpivoted_sales AS (
    SELECT
        cs."MONTH" AS period,
        wk.week,
        cs."CUSTOMER CODE" AS customer_code,
        cs."PRODUCT_CODE"  AS product_code,
        wk.value AS quantity,
        cs."PRICE / PCS VAT INC" AS price,
        cs.source_table
    FROM 
        {{ ref( 'stg_sales_combined' )}} cs
    CROSS JOIN LATERAL (
        VALUES
            ('WK 1', cs."WK 1"),
            ('WK 2', cs."WK 2"),
            ('WK 3', cs."WK 3"),
            ('WK 4', cs."WK 4"),
            ('WK 5', cs."WK 5")
    ) AS wk(week, value)
)
SELECT 
    *
FROM 
    unpivoted_sales
