
    
    

with dbt_test__target as (

  select driver_number as unique_field
  from `openf1-pipeline`.`marts`.`dim_drivers`
  where driver_number is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


