from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.filesystems import LocalFileSystem

# Import the flow function from the module where it is defined
from prefect_tasks import process_intraday_data

# Define the flow and storage location
storage = LocalFileSystem(basepath="/Users/youssefadiem/PycharmProjects/OptionsDepth_Bot")

# Create the deployment
deployment = Deployment.build_from_flow(
    flow=process_intraday_data,
    name="Intraday deployment",
    version=1,
    storage=storage,
    entrypoint="prefect_tasks.py:process_intraday_data",
    work_queue_name = 'intraday_local_youssef',
    parameters={
        # Add any necessary parameters here
    },
    schedule=CronSchedule(cron="2,12,22,32,42,52 * * * *", timezone="America/New_York")
)

if __name__ == "__main__":
    deployment.apply()
