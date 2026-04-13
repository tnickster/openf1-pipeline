

  create or replace view `openf1-pipeline`.`staging`.`stg_openf1__car_data`
  OPTIONS()
  as SELECT
    date,
    driver_number,
    rpm,
    speed,
    n_gear,
    throttle,
    brake,
    drs,
    session_key,
    meeting_key
FROM
    `openf1-pipeline`.`raw`.`car_data`;

