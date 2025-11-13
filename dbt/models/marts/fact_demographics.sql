{{config(materialized='view')}}

WITH demo AS (
    SELECT 
    *
    FROM {{ref("stg_demographics")}}
)

SELECT 
lsoa_code,
CASE
WHEN eg = 'White: Irish' THEN 1
WHEN eg = 'White: Gypsy or Irish Traveller, Roma or Other White' THEN 2
WHEN eg = 'White: English, Welsh, Scottish, Northern Irish or British' THEN 3
WHEN eg = 'Other ethnic groups: Arab, Other Asian, Other Black or any other ethnic group' THEN 7
WHEN eg = 'Mixed or Multiple ethnic groups: White and Asian, White and Black African, White and Black Caribbean or Other Mixed' THEN 11
WHEN eg = 'Black, Black British, Black Welsh, Caribbean or African: Caribbean' THEN 12
WHEN eg = 'Black, Black British, Black Welsh, Caribbean or African: African' THEN 14
WHEN eg = 'Asian, Asian British or Asian Welsh: Pakistani' THEN 15
WHEN eg = 'Asian, Asian British or Asian Welsh: Indian' THEN 16
WHEN eg = 'Asian, Asian British or Asian Welsh: Chinese' THEN 17
WHEN eg = 'Asian, Asian British or Asian Welsh: Bangladeshi' THEN 19
END AS sde_id,
pop_count
FROM demo
