WITH ss AS (SELECT * FROM {{ ref("stg_stop_search") }})

SELECT force_name
,ROW_NUMBER() OVER (ORDER BY force_name) AS force_id
FROM ss
GROUP BY force_name
ORDER BY force_name
