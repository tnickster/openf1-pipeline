
with high_values as (
    select
        date,
        brake,
        throttle,
        speed,
        rpm
    from `openf1-pipeline`.`marts`.`fct_car_telemetry`
    where brake > 105
      and throttle > 105
      and speed > 50
      and rpm > 5000
),
sustained_issues as (
    select *,
        lead(date) over (order by date) as next_reading,
        lag(date) over (order by date) as prev_reading
    from high_values
)
select
    date,
    brake,
    throttle,
    speed,
    rpm
from sustained_issues
where TIMESTAMP_DIFF(date, prev_reading, MILLISECOND) < 500
   or TIMESTAMP_DIFF(next_reading, date, MILLISECOND) < 500
