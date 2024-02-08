CREATE OR REPLACE EXTERNAL TABLE `lexical-period-413309.green_taxi_data.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny_taxi_data_dtc_bruno/green_tripdata_2022/green_tripdata_2022-*.parquet']
);

CREATE OR REPLACE TABLE lexical-period-413309.green_taxi_data.green_tripdata AS
SELECT * FROM lexical-period-413309.green_taxi_data.external_green_tripdata;

SELECT COUNT(*) 
    FROM lexical-period-413309.green_taxi_data.green_tripdata;

SELECT COUNT(DISTINCT(PULocationID)) 
    FROM lexical-period-413309.green_taxi_data.external_green_tripdata;

SELECT COUNT(DISTINCT(PULocationID)) 
    FROM lexical-period-413309.green_taxi_data.green_tripdata;

CREATE OR REPLACE TABLE lexical-period-413309.green_taxi_data.green_tripdata
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT * FROM lexical-period-413309.green_taxi_data.external_green_tripdata;

SELECT DISTINCT(PULocationID) as trips
FROM lexical-period-413309.green_taxi_data.green_tripdata_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
