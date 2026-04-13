
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select session_key
from `openf1-pipeline`.`marts`.`fct_car_telemetry`
where session_key is null



  
  
      
    ) dbt_internal_test