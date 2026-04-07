WITH laps AS(
    SELECT
        *
    FROM
        {{ ref('int_laps__enriched') }}    
)
SELECT
    *
FROM
    laps