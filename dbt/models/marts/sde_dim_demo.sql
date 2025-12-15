WITH stop_search AS (
    SELECT *
    FROM {{ref("stg_stop_search")}}
)

SELECT 
sde,
row_number() OVER (ORDER BY sde DESC) AS sde_id,
CASE 
WHEN sde_id IN (1,2,3,4) THEN 'White'
WHEN sde_id IN (5,6,7) THEN 'Other'
WHEN sde_id IN (8,9,10,11) THEN 'Mixed'
WHEN sde_id IN (12,13,14) THEN 'Black'
WHEN sde_id IN (15,16,17,18,19) THEN 'Asian'
END AS broad_sde
FROM stop_search
WHERE sde IS NOT NULL
GROUP BY sde
ORDER BY sde DESC