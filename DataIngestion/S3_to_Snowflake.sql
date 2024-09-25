-- Create Warehouse
CREATE WAREHOUSE Transforming_WH;

-- Create the database to store raw data
CREATE DATABASE Raw;

-- Create Citibike and Weather schema
CREATE SCHEMA Raw.Citibike;
CREATE SCHEMA Raw.Weather;

-- Create the Tables trips and weather data
CREATE TABLE Raw.Citibike.Trips(
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
CREATE TABLE Raw.Weather.weather_data(v variant); -- auto-detect schema

-- Create an Amazon S3 Extrernal Stage to Load data from S3
CREATE STAGE citibike_trips
URL='s3://snowflake-workshop-lab/citibike-trips';
LIST @citibike_trips;

CREATE STAGE nyc_weather_data
URL='s3://snowflake-workshop-lab/weather-nyc';
LIST @nyc_weather_data;

-- Create external file format
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
COMPRESSION = AUTO 
DATE_FORMAT = AUTO
TIMESTAMP_FORMAT = AUTO
ESCAPE_UNENCLOSED_FIELD = '\\'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ( '' );

-- Load the data from S3 into your tables
COPY INTO Raw.Citibike.Trips 
FROM @citibike_trips 
file_format = my_csv_format
PATTERN= '.*trips_.*csv.gz';

COPY INTO Raw.Weather.weather_data 
FROM @nyc_weather_data 
file_format = (type=json);

-- Check
SELECT * FROM Raw.Citibike.Trips LIMIT 10;
SELECT * FROM Raw.Weather.weather_data LIMIT 10;