{{config(materialized='view')}}

WITH lsoa AS (
    SELECT * FROM {{ref("stg_lsoa")}}
)

SELECT 
lsoa_code,
lsoa_name,
lsoa_polygon 
FROM lsoa