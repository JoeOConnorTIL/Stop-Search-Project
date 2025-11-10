{{config(materialized='view')}}

WITH demo AS (
    SELECT *
    FROM {{ref("stg_demographics")}}
)

SELECT 
eg,
row_number() OVER (ORDER BY eg DESC) AS demo_id,
CASE 
WHEN demo_id IN (1,2,3) THEN 'White'
WHEN demo_id IN (4) THEN 'Other'
WHEN demo_id IN (5) THEN 'Mixed'
WHEN demo_id IN (6,7) THEN 'Black'
WHEN demo_id IN (8,9,10,11) THEN 'Asian'
END AS broad_sde
FROM demo
GROUP BY eg
ORDER BY eg DESC