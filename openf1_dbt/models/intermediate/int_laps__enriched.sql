WITH laps AS(
    SELECT
        *
    FROM
        {{ ref('stg_openf1__laps') }}
),
drivers AS(
    SELECT
        *
    FROM
    {{ ref('stg_openf1__drivers') }}
)
SELECT
    laps.lap_number,
    laps.lap_duration,
    laps.date_start,
    laps.is_pit_out_lap,
    laps.duration_sector_1,
    laps.duration_sector_2,
    laps.duration_sector_3,
    laps.driver_number,
    laps.session_key,
    laps.meeting_key,
    drivers.full_name,
    drivers.name_acronym,
    drivers.team_name,
    drivers.team_colour
FROM 
    laps
    LEFT JOIN drivers  ON drivers.driver_number = laps.driver_number AND drivers.session_key = laps.session_key AND drivers.meeting_key = laps.meeting_key
