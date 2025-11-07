{{config(materialized='view')}}

WITH demo AS (
    SELECT 
    *
    FROM {{ref("stg_demographics")}}
),
demo_lookup AS(
    SELECT
    eg,
    row_number() OVER (ORDER BY eg DESC) AS demo_id
    FROM demo
    GROUP BY eg
    ORDER BY eg DESC
)

SELECT 
d.*,
l.demo_id
FROM demo d
JOIN demo_lookup l 
ON d.eg = l.eg