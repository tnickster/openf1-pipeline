
with validation as (
    select
        drs as drs_value
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where drs is not null
),
validation_errors as (
    select
        drs_value
    from validation
    where drs_value not in (0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15)
)
select *
from validation_errors
