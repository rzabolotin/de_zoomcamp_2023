## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

> I've created prefect workflow to load the data.   
> The workflow is in the [csv_to_gcs_flow.py](csv_to_gcs_flow.py) file.   
> The deployment was created by [make_deployment.py](make_deployment.py) script.  
> The deployment was run in a local subprocess. It loaded 12 files into the GCS bucket.

> After that I've creted two tables in BiqQuery using the folder in GCS Bucket:
> - External Table using

## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- [x] 43,244,696
- 22,978,333
- 13,942,414

```SQL 
SELECT
  COUNT(*)
FROM 
  dataset_hw3.data-native
```

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- [x] 0 MB for the External Table and 317.94MB for the BQ Table 

```sql
SELECT DISTINCT
  Affiliated_base_number
FROM 
  `dataset_hw3.data-native`;

SELECT DISTINCT
  Affiliated_base_number
FROM 
  `dataset_hw3.data-external`;


```


## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- [x] 717,748
- 1,215,687
- 5
- 20,332

```sql
SELECT 
  COUNT(*)
FROM 
  `dataset_hw3.data-native`
WHERE 
  PUlocationID is null 
  AND DOlocationID is null
```

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- [x] Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

> Partitioning is proper when we need to filter by column, so if we constantly filter by pickup_datetime it's helpful to make partitions by that column.

```sql
CREATE TABLE `dataset_hw3.data-native-clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number	
AS 
SELECT * FROM `dataset_hw3.data-native`

```

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- [x] 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

```sql
SELECT DISTINCT 
  Affiliated_base_number
FROM 
`dataset_hw3.data-native`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
```


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- [x] GCP Bucket
- Container Registry
- Big Table


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- [x] False


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 

Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur.

> I've created prefect workflow to load the data.  
> Workflow is in the [parquet_to_gcs_flow.py](parquet_to_gcs_flow.py) file.    
> The deployment was created by [make_deployment_parquet.py](make_deployment_parquet.py) script.  
> The deployment was run in a local subprocess. It loaded 12 files into the GCS bucket.
> 
> When I've tried to create BQ table using parquet files I've got an error:
>  Parquet column 'PUlocationID' has type INT64 which does not match the target cpp_type DOUBLE.
>
> The same error comes when I try to query this column in the external table. 
> So, the conclusion is that you need to match the data types in the parquet files within all files in the folder.