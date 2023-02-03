from prefect import flow, task
from prefect.tasks import task_input_hash
import pandas as pd
from prefect_gcp import GcsBucket, GcpCredentials


@task(log_prints=True, cache_key_fn=task_input_hash)
def load_data_from_s3(path):
    gcp_cloud_storage_bucket_block = GcsBucket.load("my-gcs-bucket")
    path = gcp_cloud_storage_bucket_block.download_object_to_path(f"data/{path}", path)
    df = pd.read_parquet(path)
    return df


@task(log_prints=True)
def save_to_bq(df: pd.DataFrame, table_name: str) -> None:
    creds = GcpCredentials.load("my-cred")
    print(f"Save to BQ table {table_name}")
    df.to_gbq(destination_table=table_name,
              project_id="summer-flux-373415",
              chunksize=500_000,
              credentials=creds.get_credentials_from_service_account(),
              if_exists='append'
              )


@flow(name="GCP to BQ flow")
def bg_flow(color: str, year: int, month: int) -> int:
    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"

    loaded_data = load_data_from_s3(dataset_file)
    print(f"Load {dataset_file} #rows: {loaded_data.shape}")

    save_to_bq(loaded_data, "zoom.taxiData")

    return loaded_data.shape[0]


@flow(name="Parent GCP-BQ flow", log_prints=True)
def gather_flow(color: str, year: int, months: list[int]) -> None:
    total_n_rows = 0
    for month in months:
        total_n_rows += bg_flow(color, year, month)

    print(f"Total N rows: {total_n_rows}")


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2,3]

    gather_flow(color, year, months)


