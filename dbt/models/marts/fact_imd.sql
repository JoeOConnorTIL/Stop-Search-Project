{{config(materialized='view')}}

WITH imd AS (
    SELECT *
    FROM {{ref("int_imd")}}
)

SELECT 
i.lsoa_code,
d.dep_id,
i.rank,
i.decile,
i.score
FROM imd i
LEFT JOIN imd_dim d
ON LEFT(i.dep_index, 1) = d.dep_id