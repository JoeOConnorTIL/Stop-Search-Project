{{config(materialized='view')}}

WITH raw AS (
    SELECT 
    *
    FROM {{source('raw','IMD_2019_LSOA_STAGING')}}
)

SELECT 
CAST(FEATURECODE AS STRING) AS lsoa_code,
CAST(MEASUREMENT AS STRING) AS measurement,
CAST(VALUE AS NUMBER) AS value,
CAST(INDICES_OF_DEPRIVATION AS STRING) AS dep_index
FROM RAW