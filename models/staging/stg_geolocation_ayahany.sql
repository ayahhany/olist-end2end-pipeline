{{ config(materialized='view') }}

SELECT 
    geolocation_zip_code_prefix, 
    geolocation_lat as latitude, 
    geolocation_lng as longitude, 
    geolocation_city as city, 
    geolocation_state as state 
FROM 
    project_landing.geolocation_ayahany



 