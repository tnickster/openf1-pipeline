WITH drivers AS(
    SELECT
        *
    FROM
        `openf1-pipeline`.`staging`.`stg_openf1__drivers`
)

SELECT
    *
FROM
    drivers