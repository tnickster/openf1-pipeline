
with validation as (
    select
        throttle as throttle_value
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where throttle is not null
),
validation_errors as (
    select
        throttle_value
    from validation
    where throttle_value < 0 or throttle_value > 110
)
select *
from validation_errors
