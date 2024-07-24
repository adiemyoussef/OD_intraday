import sys
import os

# Add the project root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Print PYTHONPATH for debugging
print("PYTHONPATH:", sys.path)

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.filesystems import LocalFileSystem

# Import the flow function from the module where it is defined
from prefect_flow.prefect_tasks import process_intraday_data

# Define the storage location
storage = LocalFileSystem(
    basepath="/Users/youssefadiem/PycharmProjects/OptionsDepth_intraday/prefect_deployment_storage",  # Change this to avoid conflicts
    ignore_file=".prefectignore"  # Create this file to ignore unnecessary files/folders
)

# Define the schedule
cron_schedule = CronSchedule(cron="*/10 * * * MON-FRI", timezone="America/New_York")  # Every 10 minutes, Monday to Friday

# Create the deployment
deployment = Deployment.build_from_flow(
    flow=process_intraday_data,
    name="Intraday deployment",
    version=1,
    storage=storage,
    path="prefect_flow/prefect_tasks.py",  # Specify the relative path to your flow file
    entrypoint="prefect_flow/prefect_tasks.py:process_intraday_data",  # Ensure correct relative path
    work_pool_name='intraday_local_youssef',
    parameters={
        # Add any necessary parameters here
    },
    schedule=cron_schedule
)

if __name__ == "__main__":
    deployment.apply()
