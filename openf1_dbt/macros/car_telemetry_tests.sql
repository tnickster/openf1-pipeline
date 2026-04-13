{% test brake_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as brake_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        brake_value
    from validation
    where brake_value < 0 or brake_value > 105
)
select *
from validation_errors
{% endtest %}


{% test throttle_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as throttle_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        throttle_value
    from validation
    where throttle_value < 0 or throttle_value > 110
)
select *
from validation_errors
{% endtest %}


{% test rpm_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as rpm_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        rpm_value
    from validation
    where rpm_value < 0 or rpm_value > 15000
)
select *
from validation_errors
{% endtest %}


{% test speed_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as speed_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        speed_value
    from validation
    where speed_value < 0 or speed_value > 410
)
select *
from validation_errors
{% endtest %}


{% test valid_drs(model, column_name) %}
with validation as (
    select
        {{ column_name }} as drs_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        drs_value
    from validation
    where drs_value not in (0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15)
)
select *
from validation_errors
{% endtest %}


{% test brake_throttle_combination(model) %}
with high_values as (
    select
        date,
        brake,
        throttle,
        speed,
        rpm
    from {{ model }}
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
{% endtest %}