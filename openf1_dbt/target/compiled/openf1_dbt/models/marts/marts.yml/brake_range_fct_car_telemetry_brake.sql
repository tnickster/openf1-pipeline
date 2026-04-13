
with validation as (
    select
        brake as brake_value
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where brake is not null
),
validation_errors as (
    select
        brake_value
    from validation
    where brake_value < 0 or brake_value > 105
)
select *
from validation_errors
