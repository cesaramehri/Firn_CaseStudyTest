-- Set the Role and the DW
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

-- Create the Datalake DB
CREATE DATABASE Datalake;

-- Create Citibike and Weather schema
CREATE SCHEMA Datalake.Citibike;
CREATE SCHEMA Datalake.Weather;

-- Create the Tables trips and weather data
CREATE TABLE Datalake.Citibike.Trips(
tripduration integer,   
starttime timestamp,   
stoptime timestamp,   
start_station_id integer,   
start_station_name string,   
start_station_latitude float,   
start_station_longitude float,   
end_station_id integer,
end_station_name string,   
end_station_latitude float,   
end_station_longitude float,  
bikeid integer,   
membership_type string,   
usertype string,   
birth_year integer,   
gender integer
);
CREATE TABLE Datalake.Weather.weather_data(v variant); -- auto-detect schema

-- Create an Amazon S3 Extrernal Stage to Load data from S3
CREATE STAGE citibike_trips
URL='s3://snowflake-workshop-lab/citibike-trips';
LIST @citibike_trips;

CREATE STAGE nyc_weather_data
URL='s3://snowflake-workshop-lab/weather-nyc';
LIST @nyc_weather_data;

-- Load the data from S3 into your tables
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
COMPRESSION = AUTO 
DATE_FORMAT = AUTO
TIMESTAMP_FORMAT = AUTO
ESCAPE_UNENCLOSED_FIELD = '\\'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ( '' );
  
COPY INTO Datalake.Citibike.Trips 
FROM @citibike_trips 
file_format = my_csv_format
PATTERN= '.*trips_.*csv.gz';

COPY INTO Datalake.Weather.weather_data 
FROM @nyc_weather_data 
file_format = (type=json);

-- Test
SELECT * FROM Datalake.Citibike.Trips LIMIT 10;
SELECT * FROM Datalake.Weather.weather_data LIMIT 10;

-- Create View on Weather_data
CREATE VIEW Datalake.Weather.weather_data_view 
AS SELECT   
v:time::timestamp AS observation_time,   
v:city.id::int AS city_id,   
v:city.name::string AS city_name,   
v:city.country::string AS country,   
v:city.coord.lat::float AS city_lat,   
v:city.coord.lon::float AS city_lon,   
v:clouds.all::int AS clouds,   
(v:main.temp::float)-273.15 AS temp_avg,   
(v:main.temp_min::float)-273.15 AS temp_min,
(v:main.temp_max::float)-273.15 AS temp_max,   
v:weather[0].main::string AS weather,   
v:weather[0].description::string AS weather_desc,   
v:weather[0].icon::string AS weather_icon,   
v:wind.deg::float AS wind_dir,   
v:wind.speed::float AS wind_speed 
FROM Datalake.Weather.weather_data 
WHERE city_id = 5128638;

-- Query the view
SELECT * FROM Datalake.Weather.weather_data_view 
WHERE date_trunc('month',observation_time) = '2018-01-01'  
LIMIT 20;