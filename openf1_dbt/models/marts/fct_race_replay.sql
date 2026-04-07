WITH location AS(
    SELECT
        date,
        x,
        y,
        z,
        driver_number,
        session_key,
        meeting_key,
        full_name,
        name_acronym,
        team_name,
        team_colour
    FROM
        {{ ref('int_location__enriched') }}
)

SELECT
    *
FROM
    location