
  
    

    create or replace table `openf1-pipeline`.`marts`.`dim_drivers`
      
    
    

    
    OPTIONS()
    as (
      WITH drivers AS(
    SELECT
        *
    FROM
        `openf1-pipeline`.`staging`.`stg_openf1__drivers`
)

SELECT
    *
FROM
    drivers
    );
  