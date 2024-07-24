import sys
import os
import shutil

# Add the project root directory to Python's module search path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem
from prefect.server.schemas.schedules import CronSchedule
from prefect_flow.prefect_tasks import Intraday_Flow

def create_deployment():
    # Define the path for the deployment files
    deployment_path = os.path.join(project_root, "deployment_files")

    # Remove the existing deployment directory if it exists
    if os.path.exists(deployment_path):
        shutil.rmtree(deployment_path)

    # Create a new deployment directory
    os.makedirs(deployment_path)

    # Create a LocalFileSystem block for storing flow code
    local_block = LocalFileSystem(basepath=deployment_path)

    # Create the deployment
    deployment = Deployment.build_from_flow(
        flow=Intraday_Flow,
        name="intraday_flow_deployment",
        storage=local_block,
        path="prefect_flow/prefect_tasks.py",
        entrypoint="prefect_flow.prefect_tasks:Intraday_Flow",
        schedule=CronSchedule(cron="2,12,22,32,42,52 * * * *", timezone="America/New_York")
    )

    # Apply the deployment
    deployment.apply()

if __name__ == "__main__":
    create_deployment()