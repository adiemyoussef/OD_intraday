import os
from datetime import datetime, timedelta
import pandas as pd
import asyncio
import logging
from config.config import *
from utilities.sftp_utils import SFTPUtility
from utilities.db_utils import AsyncDatabaseUtilities

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = AsyncDatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger)


async def get_distinct_sessions(sftp_utility: SFTPUtility, sftp_folder: str) -> list:
    """Detect distinct sessions in the SFTP folder."""
    try:
        files = await sftp_utility.list_directory(sftp_folder)
        sessions = set()
        for file_name in files:
            if file_name.startswith("Cboe_OpenClose_") and file_name.endswith(".csv.zip"):
                date_str = file_name.split("_")[2]
                sessions.add(date_str)
        return sorted(list(sessions), reverse=True)
    except Exception as e:
        logger.error(f"Error getting distinct sessions: {str(e)}")
        raise


async def get_initial_book(session_date: str):
    """Get the initial book for a session."""
    logger.info(f'Getting book for {session_date}')
    initial_book = await db.get_intraday_book('SPX', session_date)
    return initial_book


async def verify_and_process_message(sftp_utility: SFTPUtility, file_path: str):
    """Verify and process the file."""
    try:
        file_info = await sftp_utility.get_file_info(file_path)
        file_data = await sftp_utility.read_file(file_path)
        return file_info, file_data, file_info['mtime']
    except Exception as e:
        logger.error(f"Error verifying and processing file {file_path}: {str(e)}")
        raise


async def read_and_verify_file(file_info, file_data):
    """Read and verify the file data."""
    try:
        df = pd.read_csv(pd.compat.StringIO(file_data))
        # Add any additional verification steps here
        return df
    except Exception as e:
        logger.error(f"Error reading and verifying file: {str(e)}")
        return None


async def build_latest_book(initial_book, df):
    """Build the latest book based on initial book and new data."""
    # Implement your logic to build the latest book
    pass


async def update_book_with_latest_greeks(latest_book):
    """Update the book with the latest Greeks."""
    # Implement your logic to update the book with latest Greeks
    pass


def parse_filename(filename):
    """Parse the filename to extract date and time information."""
    parts = filename.split('_')
    date_str, time_str = parts[2], f"{parts[3]}:{parts[4]}"
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")

async def get_session_files(sftp_utility: SFTPUtility, sftp_folder: str, session_date: str):
    """Get all files for a given session in chronological order."""
    all_files = await sftp_utility.list_directory(sftp_folder)
    session_datetime = datetime.strptime(session_date, "%Y-%m-%d")

    # Filter files for the specific session date
    session_files = [f for f in all_files if f.startswith(f"Cboe_OpenClose_{session_date}")]

    # Get last modified times for session files
    file_info_list = []
    for filename in session_files:
        file_path = f"{sftp_folder}/{filename}"
        file_info = await sftp_utility.get_file_info(file_path)
        file_info_list.append((filename, file_info['mtime']))

    # Sort files based on last modified time
    sorted_files = [f[0] for f in sorted(file_info_list, key=lambda x: x[1])]

    return sorted_files

async def process_session(sftp_utility: SFTPUtility, session_date: str, sftp_folder: str):
    """Process a single session."""
    logger.info(f"Processing session for date: {session_date}")

    initial_book = await get_initial_book(session_date)
    if initial_book is None:
        logger.error(f"Cannot process session {session_date} without an initial book.")
        return

    session_files = await get_session_files(sftp_utility, sftp_folder, session_date)

    for file_name in session_files:
        file_path = f"{sftp_folder}/{file_name}"
        logger.info(f"Processing file: {file_name}")

        try:
            file_info, file_data, file_last_modified = await verify_and_process_message(sftp_utility, file_path)
            if file_info:
                logger.info(f"FILE INFO ---> {file_info}")
                df = await read_and_verify_file(file_info, file_data)
                if df is not None and not df.empty:
                    logger.info(f"Successfully processed file: {file_info['filename']}")
                    logger.info(f"DataFrame shape: {df.shape}")

                    latest_book = await build_latest_book(initial_book, df)
                    final_book = await update_book_with_latest_greeks(latest_book)
                    # Uncomment the following line when ready to insert progress
                    # await db.insert_progress('intraday', 'intraday_books', final_book)
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")


async def main():
    sftp_config = {
        "sftp_host": SFTP_HOST,
        "sftp_port": SFTP_PORT,
        "sftp_username": SFTP_USERNAME,
        "sftp_password": SFTP_PASSWORD
    }
    sftp_folder = SFTP_DIRECTORY

    async with SFTPUtility(**sftp_config, logger=logger) as sftp:
        try:
            distinct_sessions = await get_distinct_sessions(sftp, sftp_folder)
            logger.info(f"Distinct sessions: {distinct_sessions}")

            for session_date in distinct_sessions[1:-1]:
                await process_session(sftp, session_date, sftp_folder)
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())