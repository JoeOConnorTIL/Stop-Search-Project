WITH ss AS (SELECT * FROM {{ ref("stg_stop_search") }} WHERE search_object IS NOT NULL)

SELECT search_object
,ROW_NUMBER() OVER (ORDER BY search_object) AS obj_id
FROM ss
GROUP BY search_object
ORDER BY search_object