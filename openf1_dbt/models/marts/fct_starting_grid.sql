WITH starting_grid AS(
    SELECT
        *
    FROM
        {{ ref('int_starting_grid__enriched') }}
)
SELECT
    *
FROM
    starting_grid