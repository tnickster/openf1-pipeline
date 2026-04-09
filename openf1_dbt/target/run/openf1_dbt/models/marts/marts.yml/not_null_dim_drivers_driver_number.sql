
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_number
from `openf1-pipeline`.`marts`.`dim_drivers`
where driver_number is null



  
  
      
    ) dbt_internal_test