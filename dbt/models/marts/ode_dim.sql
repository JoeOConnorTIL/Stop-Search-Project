WITH ss AS (SELECT * FROM {{ ref("stg_stop_search") }} WHERE ode IS NOT NULL)

SELECT DISTINCT ode
,ROW_NUMBER() OVER (ORDER BY ode) AS ode_id
FROM ss
GROUP BY ode
ORDER BY ode
