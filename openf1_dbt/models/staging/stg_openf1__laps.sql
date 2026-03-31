SELECT
    date_start,
    driver_number,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    is_pit_out_lap,
    lap_duration,
    lap_number,
    session_key,
    meeting_key
FROM
    {{ source('openf1_raw', 'laps')}}