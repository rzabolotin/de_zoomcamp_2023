# Week 4: Analytics Engineering 

## Week 4 Homework 

Goal: Transforming the data loaded in DWH to Analytical Views developing a dbt project.

## Prerequisites
We will build a project using dbt and a running data warehouse. 
By this stage of the course you should have already: 
- A running warehouse (BigQuery or postgres) 
- A set of running pipelines ingesting the project datasets
    * Yellow taxi data - Years 2019 and 2020
    * Green taxi data - Years 2019 and 2020 
    * fhv data - Year 2019. 


> Created prefect deployment to load all the data into gcs bucket
> - [load_to_gcp.py](load_to_gcp.py)
> - [make_deployment](make_deployment.py)
>
> Run deployment to load colors ["yellow", "green", "fhv"] for required years
> 
> Created tables in BiqQuery, using files from GCS bucket.
> 
> Also, created partitioned version of tables using script:

```sql
CREATE TABLE dataset_hw4.yellow_trips_part
PARTITION BY DATE(tpep_pickup_datetime)
AS SELECT * FROM `dataset_hw4.yellow_trips`
```
 
  

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 

You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 61635151
- 61635418
- 61666551
- 41856543


### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9



### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 4208599
- 44384899
- 57084899
- 42084899


### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 22676490
- 36678853
- 22676253
- 29565253

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December
