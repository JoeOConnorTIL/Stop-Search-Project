{{ config(materialized="view") }}

WITH ss AS (SELECT * FROM {{ ref("stg_stop_search") }})

SELECT legislation
,ROW_NUMBER() OVER (ORDER BY legislation) AS leg_id
FROM ss
WHERE legislation IS NOT NULL
GROUP BY legislation
ORDER BY legislation