
with validation as (
    select
        rpm as rpm_value
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where rpm is not null
),
validation_errors as (
    select
        rpm_value
    from validation
    where rpm_value < 0 or rpm_value > 15000
)
select *
from validation_errors
