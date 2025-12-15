{{config(materialized='view')}}

WITH imd AS (
    SELECT *
    FROM {{ref("stg_imd")}}
)

SELECT 
*
FROM imd