SELECT
    date,
    driver_number,
    x,
    y,
    z,
    session_key,
    meeting_key
FROM
    {{ source('openf1_raw', 'location')}}