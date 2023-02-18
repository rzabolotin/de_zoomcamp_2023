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
    try:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    except:
        pass

    try:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    except:
        pass

    try:
        df.PULocationID = df.PULocationID.astype(pd.Int64Dtype)
        df.DOLocationID = df.DOLocationID.astype(pd.Int64Dtype)
    except:
        pass

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


@flow(name="GCP flow")
def gcp_flow(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    raw_data = ingest_data(dataset_url)
    clean_data = clean_it(raw_data)
    path = save_data_local(clean_data, "data/" + dataset_file)
    save_to_gcp(path)


@flow(name="Gather flow")
def gather_flow(colors: list[str], years: list[int]) -> None:
    for y in year:
        for c in color:
            for m in range(1,13):
                gcp_flow(c, y, m)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    month = 1

    gcp_flow(color, year, month)



