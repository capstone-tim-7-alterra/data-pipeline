from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(
        url="https://github.com/capstone-tim-7-alterra/data-pipeline.git",
        branch="main",
        credentials={
            "access_token": Secret.load("github-access-token")
        }
    ),
    entrypoint="code/main.py:data_pipeline",
    ).deploy(
        name="data-pipeline-production",
        work_pool_name="my-managed-pool",
        description="Production deployment of the data pipeline",
        tags=["etl-pipeline", "production", "automation"],
        cron="0 * * * *",
)