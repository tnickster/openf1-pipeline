SELECT
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
    {{ source('openf1_raw', 'car_data') }}