
  
    

    create or replace table `openf1-pipeline`.`staging`.`stg_openf1__starting_grid`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    position,
    driver_number,
    lap_duration,
    session_key,
    meeting_key
FROM
    `openf1-pipeline`.`raw`.`starting_grid`
    );
  