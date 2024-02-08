CREATE OR REPLACE EXTERNAL TABLE `lexical-period-413309.green_taxi_data.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `lexical-period-413309.green_taxi_data.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny_taxi_data_dtc_bruno/green_tripdata_2022/green_tripdata_2022-*.parquet']
);

CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.green_tripdata AS
SELECT * FROM taxi-rides-ny.nytaxi.external_green_tripdata;

SELECT COUNT(*) FROM lexical-period-413309.green_taxi_data.green_tripdata;

SELECT COUNT(DISTINCT(PULocationID)) FROM lexical-period-413309.green_taxi_data.external_green_tripdata;

SELECT COUNT(DISTINCT(PULocationID)) FROM lexical-period-413309.green_taxi_data.green_tripdata;