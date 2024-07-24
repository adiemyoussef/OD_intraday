from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem
from prefect.server.schemas.schedules import CronSchedule
from prefect_flow.prefect_tasks import Intraday_Flow  # Import your flow function

def create_deployment():
    # Create a LocalFileSystem block for storing flow code
    local_block = LocalFileSystem(basepath="./OptionsDepth_intraday")

    # Create the deployment
    deployment = Deployment.build_from_flow(
        flow=Intraday_Flow,
        name="your_flow_name_deployment",
        storage=local_block,
        path="prefect_flow/prefect_tasks.py",
        entrypoint="prefect_flow/prefect_tasks.py:Intraday_Flow",
        schedule=CronSchedule(cron="2,12,22,32,42,52 * * * *", timezone="America/New_York")
    )

if __name__ == "__main__":
    create_deployment()

