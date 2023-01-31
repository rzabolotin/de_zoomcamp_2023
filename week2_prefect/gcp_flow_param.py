from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
import pandas as pd
from prefect_gcp import GcsBucket, GcpCredentials


@task(retries=3, cache_key_fn=task_input_hash)
def ingest_data(url: Path) -> pd.DataFrame:
    """Ingest the data"""
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean_it(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data"""
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task
def save_data_local(df: pd.DataFrame, path: Path) -> str:
    """ Save the file locally"""
    local_file = f"{path}.parquet"
    df.to_parquet(local_file, compression="gzip")
    return local_file

@task
def save_to_gcp(path: str) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("my-gcs-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@task(log_prints=True)
def save_to_bq(df: pd.DataFrame, table_name: str) -> None:
    creds = GcpCredentials.load("my-cred")
    print(f"Save to BQ table {table_name}")
    df.to_gbq(destination_table=table_name,
              project_id="summer-flux-373415",
              chunksize=500_000,
              credentials=creds.get_credentials_from_service_account(),
              if_exists="append"
              )


@flow(name="GCP flow")
def gcp_flow(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    raw_data = ingest_data(dataset_url)
    clean_data = clean_it(raw_data)
    path = save_data_local(clean_data, "data/" + dataset_file)
    save_to_gcp(path)

    #save_to_bq(clean_data, "zoom.taxiData")


@flow(name="Parent GCP flow")
def gather_flow(color: str, year: int, months: list[int]) -> None:
    for month in months:
        gcp_flow(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1,2,3]

    gather_flow(color, year, months)



