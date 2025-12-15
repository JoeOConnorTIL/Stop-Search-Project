WITH raw AS (
    SELECT 
    *
    FROM {{source('raw','LSOA_DEMOGRAPHICS_STAGING')}}
)

SELECT 
CAST(LSOA21CD AS STRING) AS lsoa_code,
CAST("ethnic_group" AS STRING) AS eg,
CAST("count" AS NUMBER) AS pop_count
FROM raw