WITH laps AS(
    SELECT
        *
    FROM
        `openf1-pipeline`.`intermediate`.`int_laps__enriched`    
)
SELECT
    *
FROM
    laps