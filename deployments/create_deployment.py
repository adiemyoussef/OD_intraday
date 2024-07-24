import sys
import os

# Add the project root directory to the PYTHONPATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Print PYTHONPATH for debugging
print("PYTHONPATH:", sys.path)

from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from prefect.filesystems import LocalFileSystem

# Import the flow function from the module where it is defined
from prefect_flow.prefect_tasks import Intraday_Flow

# Define the storage location
storage = LocalFileSystem(
    basepath="/Users/youssefadiem/PycharmProjects/OptionsDepth_intraday/prefect_deployment_storage",  # Change this to avoid conflicts
    ignore_file=".prefectignore"  # Create this file to ignore unnecessary files/folders
)

# Define the schedules
# Schedule to run at 5 PM and 6 PM, Monday to Friday
# cron_schedule_1 = CronSchedule(cron="0 17,18 * * MON-FRI", timezone="America/New_York")
#
# # Schedule to run every 10 minutes
# cron_schedule_2 = CronSchedule(cron="2,12,22,32,42,52 * * * *", timezone="America/New_York")

# Create the deployment
deployment = Deployment.build_from_flow(
    flow=Intraday_Flow,
    name="Intraday deployment",
    version=1,
    storage=storage,
    path="prefect_flow/prefect_tasks.py",  # Specify the relative path to your flow file
    entrypoint="prefect_flow/prefect_tasks.py:Intraday_Flow",  # Ensure correct relative path
    work_pool_name='intraday_local_youssef',
    parameters={
        # Add any necessary parameters here
    },
    schedule=CronSchedule(cron="0,10,20,30,40,50 * * * *", timezone="America/New_York")  # Add both schedules here
    #schedule = [cron_schedule_1,cron_schedule_2]
)

if __name__ == "__main__":
    deployment.apply()
