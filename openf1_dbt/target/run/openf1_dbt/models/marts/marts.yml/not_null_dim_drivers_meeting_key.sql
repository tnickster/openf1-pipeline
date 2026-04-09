
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select meeting_key
from `openf1-pipeline`.`marts`.`dim_drivers`
where meeting_key is null



  
  
      
    ) dbt_internal_test