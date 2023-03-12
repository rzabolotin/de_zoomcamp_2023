gcloud dataproc jobs submit pyspark \
    --cluster=first-cluster \
    --region=us-central1 \
    gs://roman_bucket_422/perform-gcs.py \
    -- \
        --input_green=gs://roman_bucket_422/hw4/green_tripdata_2020*.parquet \
        --input_yellow=gs://roman_bucket_422/hw4/yellow_tripdata_2020*.parquet \
        --output=gs://roman_bucket_422/hw5/report2020/