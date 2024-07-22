import asyncio
import asyncssh
import io
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

class SFTPUtility:
    def __init__(self, sftp_host: str, sftp_port: int, sftp_username: str, sftp_password: str, logger: Optional[logging.Logger] = None):
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self.sftp_client = None

    async def connect(self) -> None:
        """Establish a connection to the SFTP server."""
        try:
            self.connection = await asyncssh.connect(
                self.sftp_host,
                port=self.sftp_port,
                username=self.sftp_username,
                password=self.sftp_password,
                known_hosts=None
            )
            self.sftp_client = await self.connection.start_sftp_client()
            self.logger.info("SFTP connection established successfully.")
        except Exception as e:
            self.logger.error(f"Failed to establish SFTP connection: {str(e)}")
            raise

    async def disconnect(self) -> None:
        """Close the SFTP connection."""
        if self.sftp_client:
            self.sftp_client.exit()
        if self.connection:
            self.connection.close()
        self.logger.info("SFTP connection closed.")

    async def list_directory(self, directory: str) -> List[str]:
        """List files in a directory."""
        try:
            files = await self.sftp_client.listdir(directory)
            return [f for f in files if f not in {'.', '..'}]
        except Exception as e:
            self.logger.error(f"Error listing directory {directory}: {str(e)}")
            raise

    async def read_file(self, file_path: str) -> io.BytesIO:
        """Read the contents of a file and return a file-like object."""
        try:
            async with self.sftp_client.open(file_path, 'rb') as file:
                content = await file.read()
            return io.BytesIO(content)
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    async def write_file(self, file_path: str, content: str) -> None:
        """Write content to a file."""
        try:
            async with self.sftp_client.open(file_path, 'w') as file:
                await file.write(content)
            self.logger.info(f"File {file_path} written successfully.")
        except Exception as e:
            self.logger.error(f"Error writing file {file_path}: {str(e)}")
            raise

    async def delete_file(self, file_path: str) -> None:
        """Delete a file."""
        try:
            await self.sftp_client.remove(file_path)
            self.logger.info(f"File {file_path} deleted successfully.")
        except Exception as e:
            self.logger.error(f"Error deleting file {file_path}: {str(e)}")
            raise

    async def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get information about a file."""
        try:
            file_attr = await self.sftp_client.stat(file_path)
            return {
                "file_name": file_path.split('/')[-1],
                "path": file_path,
                "size": file_attr.size,
                "mtime": datetime.fromtimestamp(file_attr.mtime).isoformat(),
                "permissions": oct(file_attr.permissions)
            }
        except Exception as e:
            self.logger.error(f"Error getting file info for {file_path}: {str(e)}")
            raise

    async def monitor_directory(self, directory: str, callback, interval: int = 60):
        """Monitor a directory for new files and call the callback function for each new file."""
        seen_files = set()
        while True:
            try:
                files = await self.list_directory(directory)
                new_files = set(files) - seen_files
                for file in new_files:
                    file_path = f"{directory}/{file}"
                    file_info = await self.get_file_info(file_path)
                    await callback(file_info)
                    seen_files.add(file)
                await asyncio.sleep(interval)
            except Exception as e:
                self.logger.error(f"Error monitoring directory {directory}: {str(e)}")
                await asyncio.sleep(interval)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

# Example usage:
async def main():
    sftp_config = {
        "sftp_host": "your_sftp_host",
        "sftp_port": 22,
        "sftp_username": "your_username",
        "sftp_password": "your_password"
    }

    async with SFTPUtility(**sftp_config) as sftp:
        # List files in a directory
        files = await sftp.list_directory("/path/to/directory")
        print(f"Files in directory: {files}")

        # Read a file
        content = await sftp.read_file("/path/to/file.txt")
        print(f"File content: {content}")

        # Write to a file
        await sftp.write_file("/path/to/newfile.txt", "Hello, SFTP!")

        # Get file info
        file_info = await sftp.get_file_info("/path/to/file.txt")
        print(f"File info: {file_info}")

        # Monitor a directory
        async def new_file_callback(file_info):
            print(f"New file detected: {file_info}")

        await sftp.monitor_directory("/path/to/monitor", new_file_callback, interval=30)

if __name__ == "__main__":
    asyncio.run(main())