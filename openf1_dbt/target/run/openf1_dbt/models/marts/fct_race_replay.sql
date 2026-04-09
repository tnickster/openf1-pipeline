
  
    

    create or replace table `openf1-pipeline`.`marts`.`fct_race_replay`
      
    
    

    
    OPTIONS()
    as (
      WITH location AS(
    SELECT
        date,
        x,
        y,
        z,
        driver_number,
        session_key,
        meeting_key,
        full_name,
        name_acronym,
        team_name,
        team_colour
    FROM
        `openf1-pipeline`.`intermediate`.`int_location__enriched`
)

SELECT
    *
FROM
    location
    );
  