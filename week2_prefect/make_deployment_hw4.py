from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from gcp_flow_hw4 import gcp_flow

my_dep = Deployment.build_from_flow(
    flow=gcp_flow,
    name="GitHub flow: hw4",
    parameters={"year":2020, "color":"green","month":11},
    storage=GitHub.load("de-zoom-repo"),
    entrypoint="week2_prefect/gcp_flow_hw4.py:gcp_flow"
)


if __name__ == "__main__":
    my_dep.apply()