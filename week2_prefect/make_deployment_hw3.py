from prefect.deployments import Deployment

from bcg_to_bq_flow_hw3 import gather_flow


my_dep = Deployment.build_from_flow(
    flow=gather_flow,
    name="GCP-to-BQ-flow",
    parameters={"year":2020, "color":"yellow","months":[1,2]}
)


if __name__ == "__main__":
    my_dep.apply()