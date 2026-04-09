
  
    

    create or replace table `openf1-pipeline`.`staging`.`stg_openf1__drivers`
      
    
    

    
    OPTIONS()
    as (
      SELECT
    driver_number,
    full_name,
    team_name,
    team_colour,
    headshot_url,
    session_key,
    meeting_key,
    name_acronym
FROM
    `openf1-pipeline`.`raw`.`drivers`
    );
  