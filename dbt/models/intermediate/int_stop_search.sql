{{config(materialized='view')}}

WITH stop_search AS (
    SELECT *,
    ST_MAKEPOINT(lon,lat) AS location_point
    FROM {{ref('stg_stop_search')}}
),
lsoa AS (
    SELECT * FROM {{ref("stg_lsoa")}}
)

SELECT 
s.*,
l.lsoa_code
FROM stop_search s
JOIN lsoa l 
ON ST_CONTAINS(l.lsoa_polygon, s.location_point)
