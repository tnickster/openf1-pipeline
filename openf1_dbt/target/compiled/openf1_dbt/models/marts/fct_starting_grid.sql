WITH starting_grid AS(
    SELECT
        *
    FROM
        `openf1-pipeline`.`intermediate`.`int_starting_grid__enriched`
)
SELECT
    *
FROM
    starting_grid