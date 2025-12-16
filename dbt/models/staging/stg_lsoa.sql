WITH raw as (
    SELECT 
    *
    FROM {{source('raw','LSOA_STAGING')}}
)

SELECT
CAST(FID AS NUMBER) AS fid,
CAST(LSOA21CD AS STRING) AS lsoa_code,
CAST(LSOA21NM AS STRING) AS lsoa_name,
CAST(LAT AS FLOAT) AS lat,
CAST(LONG AS FLOAT) AS long,
TO_GEOGRAPHY(GEOMETRY_WKT) AS lsoa_polygon
FROM raw