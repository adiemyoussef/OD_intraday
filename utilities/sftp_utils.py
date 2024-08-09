import paramiko
import io
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import time


class SFTPUtility:
    def __init__(self, sftp_host: str, sftp_port: int, sftp_username: str, sftp_password: str, logger: Optional[logging.Logger] = None):
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password
        self.logger = logger or logging.getLogger(__name__)
        self.ssh_client = None
        self.sftp_client = None

    def connect(self, time_out=60) -> None:
        """Establish a connection to the SFTP server."""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                self.sftp_host,
                port=self.sftp_port,
                username=self.sftp_username,
                password=self.sftp_password,
                timeout=time_out
            )
            self.sftp_client = self.ssh_client.open_sftp()
            self.logger.info("SFTP connection established successfully.")
        except Exception as e:
            self.logger.error(f"Failed to establish SFTP connection: {str(e)}")
            raise

    def is_connected(self):
        if self.ssh_client is None or self.sftp_client is None:
            return False
        try:
            self.sftp_client.listdir('.')
            return True
        except:
            return False

    def ensure_connection(self):
        if not self.is_connected():
            self.connect()
    def disconnect(self) -> None:
        """Close the SFTP connection."""
        if self.sftp_client:
            self.sftp_client.close()
        if self.ssh_client:
            self.ssh_client.close()
        self.logger.info("SFTP connection closed.")

    def list_directory(self, directory: str) -> List[str]:
        """List files in a directory."""
        try:
            files = self.sftp_client.listdir(directory)
            return [f for f in files if f not in {'.', '..'}]
        except Exception as e:
            self.logger.error(f"Error listing directory {directory}: {str(e)}")
            raise

    def read_file(self, file_path: str) -> io.BytesIO:
        """Read the contents of a file and return a file-like object."""
        try:
            with self.sftp_client.file(file_path, 'rb') as file:
                content = file.read()
            return io.BytesIO(content)
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def write_file(self, file_path: str, content: str) -> None:
        """Write content to a file."""
        try:
            with self.sftp_client.file(file_path, 'w') as file:
                file.write(content)
            self.logger.info(f"File {file_path} written successfully.")
        except Exception as e:
            self.logger.error(f"Error writing file {file_path}: {str(e)}")
            raise

    def delete_file(self, file_path: str) -> None:
        """Delete a file."""
        try:
            self.sftp_client.remove(file_path)
            self.logger.info(f"File {file_path} deleted successfully.")
        except Exception as e:
            self.logger.error(f"Error deleting file {file_path}: {str(e)}")
            raise

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get information about a file."""
        try:
            file_attr = self.sftp_client.stat(file_path)
            return {
                "file_name": file_path.split('/')[-1],
                "path": file_path,
                "size": file_attr.st_size,
                "mtime": datetime.fromtimestamp(file_attr.st_mtime).isoformat(),
                "permissions": oct(file_attr.st_mode)
            }
        except Exception as e:
            self.logger.error(f"Error getting file info for {file_path}: {str(e)}")
            raise

    def monitor_directory(self, directory: str, callback, interval: int = 60):
        """Monitor a directory for new files and call the callback function for each new file."""
        seen_files = set()
        while True:
            try:
                files = self.list_directory(directory)
                new_files = set(files) - seen_files
                for file in new_files:
                    file_path = f"{directory}/{file}"
                    file_info = self.get_file_info(file_path)
                    callback(file_info)
                    seen_files.add(file)
                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"Error monitoring directory {directory}: {str(e)}")
                time.sleep(interval)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

# Example usage:
def main():
    sftp_config = {
        "sftp_host": "your_sftp_host",
        "sftp_port": 22,
        "sftp_username": "your_username",
        "sftp_password": "your_password"
    }

    with SFTPUtility(**sftp_config) as sftp:
        # List files in a directory
        files = sftp.list_directory("/path/to/directory")
        print(f"Files in directory: {files}")

        # Read a file
        content = sftp.read_file("/path/to/file.txt")
        print(f"File content: {content.getvalue().decode('utf-8')}")

        # Write to a file
        sftp.write_file("/path/to/newfile.txt", "Hello, SFTP!")

        # Get file info
        file_info = sftp.get_file_info("/path/to/file.txt")
        print(f"File info: {file_info}")

        # Monitor a directory
        def new_file_callback(file_info):
            print(f"New file detected: {file_info}")

        # Note: This will run indefinitely
        sftp.monitor_directory("/path/to/monitor", new_file_callback, interval=30)

if __name__ == "__main__":
    main()