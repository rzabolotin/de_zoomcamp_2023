from prefect.deployments import Deployment
from parquet_to_gcs_flow import load_files_to_cloud

my_dep = Deployment.build_from_flow(
    flow=load_files_to_cloud,
    name="Local deployment",
    parameters={"prefix":"fhv_tripdata", "year":2019, "months":list(range(1,13))}
)

if __name__ == "__main__":
    my_dep.apply()