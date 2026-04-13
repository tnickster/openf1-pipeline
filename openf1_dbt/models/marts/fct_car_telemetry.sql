WITH car_data AS (
    SELECT
        *
    FROM
        {{ ref('stg_openf1__car_data') }}
),

drivers AS (
    SELECT
        driver_number,
        full_name,
        name_acronym,
        team_name,
        team_colour,
        meeting_key
    FROM
        {{ ref('dim_drivers') }}
)

SELECT
    car_data.date,
    car_data.driver_number,
    car_data.rpm,
    car_data.speed,
    car_data.n_gear,
    car_data.throttle,
    car_data.brake,
    car_data.drs,
    car_data.session_key,
    car_data.meeting_key,
    drivers.full_name,
    drivers.name_acronym,
    drivers.team_name,
    drivers.team_colour
FROM
    car_data
LEFT JOIN
    drivers
    ON car_data.driver_number = drivers.driver_number
    AND car_data.meeting_key = drivers.meeting_key