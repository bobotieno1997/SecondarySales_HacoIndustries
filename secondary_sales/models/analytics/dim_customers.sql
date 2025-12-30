{{ config(
    materialized='table',
    schema='analytics',
    alias='dim_customers',
    tags=['staging', 'dim_customers'],
    on_schema_change='fail'
) }}

WITH customers AS (
  SELECT 
      customer_code,
      customer_name,
      CASE 
          WHEN customer_code IN (
              'DS007001L','DS017002L','DS076001L'
          ) THEN 'C. Rift & S. Nyanza'

          WHEN customer_code IN (
              'DS010101L','DS089001L','DSO56001L'
          ) THEN 'CENTRAL KENYA'

          WHEN customer_code IN (
              'DS015001L','DS069001L','DS071001L',
              'DS100001L','OT228001L'
          ) THEN 'COAST'

          WHEN customer_code IN (
              'DS013001L','DS016001L'
          ) THEN 'EASTERN'

          WHEN customer_code IN (
              'BS028001L','DS001001L','DS086001L'
          ) THEN 'MOUNTAIN'

          WHEN customer_code IN (
              'BS025001L','DS009001L','DS046001L',
              'DS085001L','DS090001L','DS094001L',
              'DS097001L'
          ) THEN 'NAIROBI'

          WHEN customer_code = 'DS010001L' THEN 'NORTH RIFT'

          WHEN customer_code IN (
              'DS017001L','DS099001L'
          ) THEN 'NYANZA & WESTERN'

          ELSE 'UNKNOWN'
      END AS customer_region
  FROM 
    {{ ref('customers') }}
  ORDER BY customer_name ASC
)
SELECT 
	ROW_NUMBER()OVER()AS customer_id,
	customer_code,
  customer_name,
  INITCAP(customer_region)
FROM
	customers

    