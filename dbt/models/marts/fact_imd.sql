{{config(materialized='view')}}

WITH imd AS (
    SELECT *
    FROM {{ref("int_imd")}}
)

SELECT 
lsoa21cd AS lsoa_Code,
imd_rank,
imd_decile
FROM imd