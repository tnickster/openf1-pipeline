
  
    

    create or replace table `openf1-pipeline`.`staging`.`stg_openf1__location`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    date,
    driver_number,
    x,
    y,
    z,
    session_key,
    meeting_key
FROM
    `openf1-pipeline`.`raw`.`location`
    );
  