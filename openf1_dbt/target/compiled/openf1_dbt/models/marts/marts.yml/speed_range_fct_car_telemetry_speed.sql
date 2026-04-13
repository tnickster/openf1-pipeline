
with validation as (
    select
        speed as speed_value
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where speed is not null
),
validation_errors as (
    select
        speed_value
    from validation
    where speed_value < 0 or speed_value > 410
)
select *
from validation_errors
