{{ config(
    materialized='table',
    schema='staging',
    alias='consolidated_stock',
    tags=['staging', 'consolidated_stock'],
    on_schema_change='fail'
) }}

-- Get list of sales tables dynamically from information schema
{% set get_tables_query %}
SELECT 
    table_name
FROM 
    information_schema.tables
WHERE 
    table_schema = 'raw'  
AND 
    table_name LIKE 'stock%'  
ORDER BY 
    table_name
{% endset %}

-- Run query only during execution phase, not parsing
{% if execute %}
    {% set results = run_query(get_tables_query) %}
    {% set sales_tables = results.columns[0].values() %}
{% else %}
    {% set sales_tables = [] %}
{% endif %}

-- Append all sales tables dynamically
{% for table in sales_tables %}
SELECT 
    CAST("TYPE" AS TEXT),
    CAST("MONTH" AS DATE),
    CAST("CUSTOMER CODE" AS TEXT),
    CAST("CUSTOMER NAME" AS TEXT),
    CAST("BRAND" AS TEXT),
    CAST("DESCRIPTION" AS TEXT),
    CAST("PRODUCT_CODE" AS BIGINT),
    CAST("VARIANT" AS TEXT),
    CAST("CATEGORY" AS TEXT),
    CAST("SKU" AS TEXT),
    CAST("PAC" AS SMALLINT),
    CAST("WK 1" AS FLOAT),
    CAST("WK 2" AS FLOAT),
    CAST("WK 3" AS FLOAT),
    CAST("WK 4" AS FLOAT),
    CAST("WK 5" AS FLOAT),
    CAST("PRICE / PCS VAT INC" AS DOUBLE PRECISION),
    '{{ table }}' AS source_table,
    CURRENT_TIMESTAMP AS loaded_at
FROM 
    raw.{{ table }}
WHERE 
    "TYPE" IS NOT NULL
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
