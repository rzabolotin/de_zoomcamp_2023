from prefect.deployments import Deployment
from load_to_gcp import gather_flow

my_dep = Deployment.build_from_flow(
    flow=gather_flow,
    name="Local deployment",
    parameters={"colors": ["green","yellow"], "years": [2019, 2020]}
)

if __name__ == "__main__":
    my_dep.apply()
