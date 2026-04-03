-- depends_on: {{ ref('stg_openf1__location') }}

WITH location AS(
    SELECT
        *
    FROM
        {{ ref('stg_openf1__location') }}
),

drivers AS(
    SELECT
        *
    FROM
    {{ ref('stg_openf1__drivers')}}
),

location_enriched AS (
    SELECT
        location.*,
        drivers.full_name,
        drivers.team_name,
        drivers.team_colour,
        drivers.headshot_url
    FROM location
        LEFT JOIN drivers ON location.session_key = drivers.session_key AND location.driver_number = drivers.driver_number
)

SELECT * FROM location_enriched