
  
    

    create or replace table `openf1-pipeline`.`staging`.`stg_openf1__laps`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    date_start,
    driver_number,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    is_pit_out_lap,
    lap_duration,
    lap_number,
    session_key,
    meeting_key
FROM
    `openf1-pipeline`.`raw`.`laps`
    );
  