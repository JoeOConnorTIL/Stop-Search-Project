{{config(materialized='view')}}

WITH stop_search AS (
    SELECT *,
    ST_MAKEPOINT(lon,lat) AS location_point
    FROM {{ref('stg_stop_search')}}
),
lsoa AS (
    SELECT * FROM {{ref("stg_lsoa")}}
),
ode_dim AS (
    SELECT * FROM {{ref("ode_dim")}}
),
sde_dim AS (
    SELECT * FROM {{ref("sde_dim")}}
),
forces_dim AS (
    SELECT * FROM {{ref("forces_dim")}}
),
legislation_dim AS (
    SELECT * FROM {{ref("legislation_dim")}}
),
outcome_dim AS (
    SELECT * FROM {{ref("outcome_dim")}}
),
search_object_dim AS (
    SELECT * FROM {{ref("search_object_dim")}}
)

SELECT 
s.age_range,
oc.outcome_id,
sd.sde_id,
o.ode_id,
s.gender,
lg.leg_id,
s.outcome_linked_to_object,
s.removal_clothing,
s.search_date,
s.location_point,
s.street_id,
s.street_name,
CASE 
WHEN s.search_type = 'Person search' THEN FALSE
WHEN s.search_type = 'Person and Vehicle search' THEN TRUE
END AS vehicle_in_search,
so.obj_id,
f.force_id,
l.lsoa_code
FROM stop_search s
LEFT JOIN lsoa l 
ON ST_CONTAINS(l.lsoa_polygon, s.location_point)
LEFT JOIN ode_dim o
ON s.ode = o.ode
LEFT JOIN sde_dim sd
ON s.sde = sd.sde
LEFT JOIN forces_dim f
ON s.force_name = f.force_name
LEFT JOIN legislation_dim lg
ON s.legislation = lg.legislation
LEFT JOIN outcome_dim oc
ON s.outcome = oc.outcome
LEFT JOIN search_object_dim so
ON s.search_object = so.search_object
WHERE s.ode IS NOT NULL AND s.involved_person = true
