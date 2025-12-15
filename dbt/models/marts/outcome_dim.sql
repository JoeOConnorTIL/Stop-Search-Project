WITH ss AS (SELECT * FROM {{ ref("stg_stop_search") }} WHERE outcome IS NOT NULL AND TRIM(outcome) <> '')

SELECT outcome
,ROW_NUMBER() OVER (ORDER BY outcome) AS outcome_id
FROM ss
GROUP BY outcome
ORDER BY outcome