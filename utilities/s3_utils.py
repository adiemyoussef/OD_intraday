import logging
import json
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class S3Utilities:
    """
    A utility class for interacting with S3-compatible storage services.

    This class provides methods for loading and saving JSON data to S3 buckets,
    with specific support for DigitalOcean Spaces.
    """

    def __init__(self, endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str, bucket: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the S3Utilities instance.

        :param endpoint_url: The URL of the S3-compatible service endpoint
        :param aws_access_key_id: The access key for the S3 service
        :param aws_secret_access_key: The secret key for the S3 service
        :param bucket: The name of the S3 bucket to use
        :param logger: Logger instance to use for logging. If None, a new logger will be created.
        """
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket = bucket
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info(f"S3Utilities initialized with bucket: {bucket}")

    def load_json(self, key: str) -> Dict[str, Any]:
        """
        Load JSON data from an S3 object.

        :param key: The key (path) of the object in the S3 bucket
        :return: A dictionary containing the loaded JSON data, or an empty dict if the file is not found or an error occurs

        This method attempts to load a JSON file from the specified S3 bucket and key.
        If the file is not found, it logs an info message and returns an empty dictionary.
        If any other error occurs during the process, it logs an error message and returns an empty dictionary.

        Exceptions handled:
        - botocore.exceptions.ClientError: For S3-specific errors (including NoSuchKey)
        - NoCredentialsError: If AWS credentials are missing or invalid
        - json.JSONDecodeError: If the loaded data is not valid JSON
        """
        try:
            self.logger.debug(f"Attempting to load JSON from {key}")
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            self.logger.debug(f"Successfully loaded JSON data from {key}")
            return data
        except self.s3_client.exceptions.NoSuchKey:
            self.logger.info(f"File {key} not found in bucket. Returning empty dict.")
            return {}
        except NoCredentialsError as e:
            self.logger.error(f"No valid AWS credentials found: {e}")
            return {}
        except ClientError as e:
            self.logger.error(f"AWS client error occurred while loading JSON from {key}: {e}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from {key}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while loading JSON from {key}: {e}")
            return {}

    def save_json(self, key: str, data: Dict[str, Any]) -> bool:
        """
        Save JSON data to an S3 object.

        :param key: The key (path) where the object should be saved in the S3 bucket
        :param data: A dictionary containing the data to be saved as JSON
        :return: True if the save operation was successful, False otherwise

        This method attempts to save the provided dictionary as a JSON file to the specified S3 bucket and key.
        If any error occurs during the process, it logs an error message and returns False.

        Exceptions handled:
        - NoCredentialsError: If AWS credentials are missing or invalid
        - ClientError: For any AWS-specific errors
        - TypeError: If the data is not JSON serializable
        """
        try:
            self.logger.debug(f"Attempting to save JSON data to {key}")
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType='application/json'
            )
            self.logger.info(f"Successfully saved JSON data to {key}")
            return True
        except NoCredentialsError as e:
            self.logger.error(f"No valid AWS credentials found: {e}")
            return False
        except ClientError as e:
            self.logger.error(f"AWS client error occurred while saving JSON to {key}: {e}")
            return False
        except TypeError as e:
            self.logger.error(f"Error encoding data to JSON for {key}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while saving JSON to {key}: {e}")
            return False