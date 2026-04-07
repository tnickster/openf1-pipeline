WITH drivers AS(
    SELECT
        *
    FROM
        {{ ref('stg_openf1__drivers') }}
)

SELECT
    *
FROM
    drivers