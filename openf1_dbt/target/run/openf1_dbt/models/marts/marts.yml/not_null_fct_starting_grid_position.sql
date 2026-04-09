
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select position
from `openf1-pipeline`.`marts`.`fct_starting_grid`
where position is null



  
  
      
    ) dbt_internal_test