SELECT
    driver_number,
    full_name,
    team_name,
    team_colour,
    headshot_url,
    session_key,
    meeting_key
FROM
    {{ source('openf1_raw', 'drivers') }}