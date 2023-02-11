import os
import asyncio

from prefect import task, flow, get_run_logger
from prefect_gcp import GcsBucket


SOURCE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
DATA_FOLDER = "data/"


@task(name="Loading file to local folder")
def save_file_locally(filename):
    logger = get_run_logger()
    logger.info(f"workdir {os.getcwd()}")
    logger.info(f"Downloading file {filename}.")
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.system(f"wget {SOURCE_URL}{filename} -O {DATA_FOLDER}{filename}")
    return filename


@task()
def load_file_to_cloud(filename: str) -> None:
    logger = get_run_logger()
    logger.info(f"Uploading file {filename} to GCS.")
    bucket = GcsBucket.load("gcloud-bucket")
    bucket.upload_from_path(from_path=f"{DATA_FOLDER}{filename}", to_path=filename)


@flow(name = "loadFileToGCS")
def load_files_to_cloud(prefix: str, year: int, months: list) -> None:
    files = [f"{prefix}_{year}-{month:02}.csv.gz" for month in months]
    for file in files:
        filename = save_file_locally(file)
        load_file_to_cloud(filename)


if __name__ == "__main__":
    load_files_to_cloud("fhv_tripdata", 2019, [1,2])

