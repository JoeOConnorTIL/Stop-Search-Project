{{config(materialized='view')}}

WITH imd AS (
    SELECT *
    FROM {{ref("int_imd")}}
)

SELECT 
dep_index, 
LEFT(dep_index, 1) AS dep_id
FROM imd
GROUP BY dep_index
ORDER BY dep_index