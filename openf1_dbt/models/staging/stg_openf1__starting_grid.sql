SELECT
    position,
    driver_number,
    lap_duration,
    session_key,
    meeting_key
FROM
    {{ source('openf1_raw', 'starting_grid') }}