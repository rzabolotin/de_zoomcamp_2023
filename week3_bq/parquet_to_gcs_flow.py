import os
import pandas

from prefect import task, flow, get_run_logger
from prefect_gcp import GcsBucket

SOURCE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"


@task(name="Loading file to local folder")
def save_file_locally(filename):
    logger = get_run_logger()
    logger.info(f"workdir {os.getcwd()}")
    logger.info(f"Downloading file {filename}.")
    os.system(f"wget {SOURCE_URL}{filename} -O {filename}")
    return filename


@task(name="Converting file to parquet")
def convert_to_parquet(filename):
    logger = get_run_logger()
    logger.info(f"Converting file {filename} to parquet.")
    df = pandas.read_csv(filename)
    pq_filename = filename.replace(".csv.gz", ".parquet")
    df.to_parquet(pq_filename)
    return pq_filename


@task(name="Loading file to GCS")
def load_file_to_cloud(filename: str) -> None:
    logger = get_run_logger()
    logger.info(f"Uploading file {filename} to GCS.")
    bucket = GcsBucket.load("gcloud-bucket-parquet")
    bucket.upload_from_path(from_path=filename, to_path=filename)


@flow(name="loadParquetToGCS")
def load_files_to_cloud(prefix: str, year: int, months: list) -> None:
    files = [f"{prefix}_{year}-{month:02}.csv.gz" for month in months]
    for file in files:
        filename = save_file_locally(file)
        parquet_file = convert_to_parquet(filename)
        load_file_to_cloud(parquet_file)


if __name__ == "__main__":
    load_files_to_cloud("fhv_tripdata", 2019, [1, 2])
