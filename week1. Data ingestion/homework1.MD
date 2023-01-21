## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- [x] `--iidfile string`
- `--idimage string`
- `--idfile string`

> `docker build --help | grep Write`  
> `--iidfile string          Write the image ID to the file`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an iterative mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- [x] 3
- 7

> `pip list`
> ```
> Package    Version
> ---------- -------
> pip        22.0.4
> setuptools 58.1.0
> wheel      0.38.4
> ```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## My decision
1. Use `docker-compose.yaml` from course to run pgdatabase & pgadmin
2. Put some code to `ingest_data.py` to be able to load both files with it
3. Build docker image with 'Dockerfile', call image data_ingestion
4. Run `docker-compose up -d`
5. Run commands to ingest data
```
docker run --rm --network week1dataingestion_default data_ingestion \
 --user root \
 --password root \
 --host pgdatabase \
 --port 5432 \
 --db ny_taxi \
 --table_name taxi_data \
 --convert_dates\
 --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
 
docker run --rm --network week1dataingestion_default data_ingestion \
 --user root \
 --password root \
 --host pgdatabase \
 --port 5432 \
 --db ny_taxi \
 --table_name taxi_zones \
 --url https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Connect to the pgadmin server using 8080 port


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- [x] 20530
- 17630
- 21090

```postgresql
SELECT
    COUNT(*)
FROM
    ny_taxi.public.taxi_data AS t
WHERE
  t.lpep_pickup_datetime > '2019-01-15'
  AND t.lpep_dropoff_datetime < '2019-01-16'
```

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- [x] 2019-01-15
- 2019-01-10

```postgresql
SELECT
    t.lpep_pickup_datetime as day,
    t.trip_distance
FROM
    ny_taxi.public.taxi_data as t
ORDER BY
    trip_distance DESC
LIMIT 5;
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- [x] 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274

```postgresql
SELECT
    t.passenger_count,
    COUNT(*)
FROM
    ny_taxi.public.taxi_data AS t
WHERE
  t.lpep_pickup_datetime >= '2019-01-01'
  AND t.lpep_pickup_datetime < '2019-01-02'
GROUP BY
    t.passenger_count;
```

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- [x] Long Island City/Queens Plaza


```postgresql
SELECT
    t.tip_amount,
    pu_zone."Zone" as pu_zone,
    do_zone."Zone" as do_zone
FROM
    ny_taxi.public.taxi_data t
LEFT JOIN taxi_zones pu_zone on t."PULocationID" = pu_zone."LocationID"
LEFT JOIN taxi_zones do_zone on t."DOLocationID" = do_zone."LocationID"

WHERE pu_zone."Zone" = 'Astoria'
ORDER BY
    t.tip_amount DESC
LIMIT 5;
```

## Submitting the solutions

* Form for submitting: TBA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET
