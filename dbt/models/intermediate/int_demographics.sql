{{config(materialized='view')}}

WITH demo AS (
    SELECT 
    *
    FROM {{ref("stg_demographics")}}
)
,
demo_lookup AS (
    SELECT *
    FROM {{ref("sde_dim")}}
)

SELECT 
d.lsoa_code,
l.demo_id,
d.pop_count
FROM demo d
JOIN demo_lookup l 
ON d.eg = l.eg