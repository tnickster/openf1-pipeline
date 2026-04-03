WITH starting_grid AS(
    SELECT
        *
    FROM
        {{ ref('stg_openf1__starting_grid') }}
),
drivers AS(
    SELECT
        *
    FROM
        {{ ref('stg_openf1__drivers') }}
)
SELECT
    starting_grid.position,
    starting_grid.meeting_key,
    starting_grid.session_key,
    starting_grid.driver_number,
    drivers.full_name,
    drivers.name_acronym,
    drivers.team_name,
    drivers.team_colour
FROM
    starting_grid
    LEFT JOIN drivers ON drivers.driver_number = starting_grid.driver_number AND drivers.session_key = starting_grid.session_key AND drivers.meeting_key = starting_grid.meeting_key