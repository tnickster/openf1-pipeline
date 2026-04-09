
  
    

    create or replace table `openf1-pipeline`.`marts`.`fct_laps`
      
    
    

    
    OPTIONS()
    as (
      WITH laps AS(
    SELECT
        *
    FROM
        `openf1-pipeline`.`intermediate`.`int_laps__enriched`    
)
SELECT
    *
FROM
    laps
    );
  