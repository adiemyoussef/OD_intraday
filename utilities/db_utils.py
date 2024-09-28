import csv
import json
import math
from io import StringIO
from typing import Optional, Union, List, Dict
import aiomysql
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import pandas as pd
import logging
import asyncio
import mysql.connector
from enum import Enum
from tqdm import tqdm
from psycopg2 import sql,errors
import boto3
from botocore.client import Config
from enum import Enum
import logging
from typing import Optional

class DatabaseStatus(Enum):
    DISCONNECTED = "Disconnected"
    CONNECTED = "Connected"
    ERROR = "Error"

class SpacesStatus(Enum):
    DISCONNECTED = "Disconnected"
    CONNECTED = "Connected"
    ERROR = "Error"

class AsyncDatabaseUtilities:
    """
    A utility class for asynchronous database operations.

    This class provides methods for executing queries, inserting data, and fetching results
    from a MySQL database using asynchronous operations.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the AsyncDatabaseUtilities instance.

        :param host: Database host address
        :param port: Database port number
        :param user: Database username
        :param password: Database password
        :param database: Database name
        :param logger: Logger instance to use for logging. If None, a new logger will be created.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None
        self.logger = logger or logging.getLogger(__name__)

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
    async def create_pool(self):
        """
        Create a connection pool to the database.

        This method should be called before any other database operations.
        It creates a pool of connections that can be used for executing queries.

        Exceptions:
        - aiomysql.Error: If there's an error connecting to the database.
        - Exception: For any other unexpected errors during pool creation.
        """
        if self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.database,
                    charset='utf8mb4',
                    autocommit=True,
                    cursorclass=aiomysql.DictCursor,
                    minsize=1,
                    maxsize=10,
                    loop=self.loop or asyncio.get_event_loop()
                )
                self.logger.info("Database connection pool created successfully.")
            except aiomysql.Error as e:
                self.logger.error(f"MySQL error occurred while creating pool: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error occurred while creating pool: {e}")
                raise

    async def execute_query(
            self,
            query: str,
            params: Optional[tuple] = None,
            return_type: str = 'dataframe'
    ) -> Union[pd.DataFrame, List[Dict], List[tuple]]:
        """
        Execute a SQL query and return the results in the specified format.

        :param query: SQL query string to execute
        :param params: Optional tuple of parameters to use with the query
        :param return_type: Type of return value. Options: 'dataframe' (default), 'dict', 'tuple'
        :return: Query results in the specified format

        Exceptions:
        - aiomysql.Error: If there's an error executing the query.
        - ValueError: If an invalid return_type is specified.
        - Exception: For any other unexpected errors during query execution.
        """
        if not self.pool:
            await self.create_pool()

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    results = await cursor.fetchall()
                    columns = [column[0] for column in cursor.description]

                    if return_type.lower() == 'dataframe':

                        result = pd.DataFrame(results, columns=columns)
                        self.logger.info(f"Executed Query: {result.head()}")
                        return result
                    elif return_type.lower() == 'dict':
                        return [dict(zip(columns, row)) for row in results]
                    elif return_type.lower() == 'tuple':
                        return results
                    else:
                        raise ValueError(
                            f"Invalid return_type: {return_type}. Valid options are 'dataframe', 'dict', or 'tuple'.")

        except aiomysql.Error as e:
            self.logger.error(f"MySQL error occurred while executing query: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while executing query: {e}")
            raise
        finally:
            self.logger.info(f"Query executed: {query[:50]}...")
    async def execute_insert(self, query: str, params: tuple) -> bool:
        """
        Execute an INSERT SQL query.

        :param query: INSERT SQL query string to execute
        :param params: Tuple of parameters to use with the query
        :return: True if the insert was successful, False otherwise

        Exceptions:
        - aiomysql.Error: If there's an error executing the insert query.
        - Exception: For any other unexpected errors during query execution.
        """
        if not self.pool:
            await self.create_pool()

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    await conn.commit()
                    self.logger.info(f"Insert query executed successfully: {query[:50]}...")
                    return True
        except aiomysql.Error as e:
            self.logger.error(f"MySQL error occurred while executing insert: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while executing insert: {e}")
            return False

    async def get_initial_book(self, ticker: str, active_effective_date: str) -> pd.DataFrame:
        """
        Fetch book data for a specific ticker and date.

        :param ticker: Stock ticker symbol
        :param active_effective_date: Effective date for the book data
        :return: pandas DataFrame containing the book data

        Exceptions:
        - Same as fetch_dataframe method.
        """
        query = f"""SELECT * FROM intraday.new_daily_book_format WHERE effective_date = '{active_effective_date}' AND ticker = '{ticker}';"""
        self.logger.info(f"Fetching book data for ticker {ticker} on date {active_effective_date}")
        return await self.execute_query(query)

    async def insert_progress(self, dbName: str, dbTable: str, dataframe: pd.DataFrame):
        """
        Insert a large DataFrame into the database in chunks.

        :param dbName: Database name
        :param dbTable: Table name
        :param dataframe: pandas DataFrame to insert

        This method splits the DataFrame into chunks and inserts them separately to avoid
        overwhelming the database with a single large insert operation.

        Exceptions:
        - aiomysql.Error: If there's an error during the insert operation.
        - Exception: For any other unexpected errors during the insert process.
        """
        if not self.pool:
            await self.create_pool()

        chunksize = math.ceil(len(dataframe) / 50)
        self.logger.info(
            f"Inserting DataFrame with {len(dataframe)} rows into {dbName}.{dbTable} in chunks of {chunksize}")

        for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
            try:
                async with self.pool.acquire() as conn:
                    await conn.begin()
                    await conn.execute(f"INSERT INTO {dbName}.{dbTable} VALUES ", cdf.to_dict('records'))
                    await conn.commit()
                self.logger.info(f"Inserted chunk {i + 1} ({len(cdf)} rows)")
            except aiomysql.Error as e:
                self.logger.error(f"MySQL error occurred while inserting chunk {i}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error occurred while inserting chunk {i}: {e}")

    @staticmethod
    def chunker(df: pd.DataFrame, size: int):
        """
        Split a DataFrame into chunks of specified size.

        :param df: pandas DataFrame to split
        :param size: Size of each chunk
        :return: Generator yielding DataFrame chunks
        """
        return (df[pos:pos + size] for pos in range(0, len(df), size))

class DatabaseUtilities:
    """
    A utility class for synchronous database operations with status tracking.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logger or logging.getLogger(__name__)
        self.status = DatabaseStatus.DISCONNECTED
        self.last_error = None
        self.chunksize_divider = 50

    def connect(self):
        if self.connection is None:
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.status = DatabaseStatus.CONNECTED
                self.logger.info("Database connection created successfully.")
            except mysql.connector.Error as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"MySQL error occurred while creating connection: {e}")
                raise
            except Exception as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"Unexpected error occurred while creating connection: {e}")
                raise

    def execute_query(
            self,
            query: str,
            #params: Optional[tuple] = None,
            params: Optional[Union[tuple, dict]] = None,
            return_type: str = 'dataframe'
    ) -> Union[pd.DataFrame, List[Dict], List[tuple]]:
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor(dictionary=True) as cursor:
                # Check the type of params and execute accordingly
                if isinstance(params, dict):
                    cursor.execute(query, params)
                elif isinstance(params, tuple) or params is None:
                    cursor.execute(query, params)
                else:
                    raise ValueError("params must be either a dictionary, tuple, or None")

                results = cursor.fetchall()
                columns = [column[0] for column in cursor.description]

                if return_type.lower() == 'dataframe':
                    result = pd.DataFrame(results)
                    self.logger.info(f"Executed Query: {result.head()}")
                    return result
                elif return_type.lower() == 'dict':
                    return results
                elif return_type.lower() == 'tuple':
                    return [tuple(row.values()) for row in results]
                else:
                    raise ValueError(
                        f"Invalid return_type: {return_type}. Valid options are 'dataframe', 'dict', or 'tuple'.")

        except mysql.connector.Error as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"MySQL error occurred while executing query: {e}")
            raise
        except Exception as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Unexpected error occurred while executing query: {e}")
            raise
        finally:
            print(f"Query executed: {query}...")

    def execute_insert(self, query: str, params: tuple) -> bool:
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                self.logger.info(f"Insert query executed successfully: {query[:50]}...")
                return True
        except mysql.connector.Error as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"MySQL error occurred while executing insert: {e}")
            return False
        except Exception as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Unexpected error occurred while executing insert: {e}")
            return False

    def get_initial_book(self, ticker: str, active_effective_date: str) -> pd.DataFrame:
        query = f"""SELECT * FROM intraday.new_daily_book_format WHERE effective_date = '{active_effective_date}' AND ticker = '{ticker}';"""
        self.logger.info(f"Fetching book data for ticker {ticker} on date {active_effective_date}")
        return self.execute_query(query)

    def chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def insert_progress(self, dbName: str, dbTable: str, dataframe: pd.DataFrame):
        if not self.connection:
            self.connect()

        chunksize = math.ceil(len(dataframe) / self.chunksize_divider)
        self.logger.info(
            f"Inserting DataFrame with {len(dataframe)} rows into {dbName}.{dbTable} in chunks of {chunksize}")

        with tqdm(total=len(dataframe), desc="Inserting data") as pbar:
            for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
                try:

                    # inserted = cdf.to_sql(dbTable, schema=dbName, con = self.engine, index=False, if_exists='append')
                    # pbar.update(chunksize)
                    with self.connection.cursor() as cursor:
                        placeholders = ', '.join(['%s'] * len(cdf.columns))
                        columns = ', '.join(cdf.columns)
                        query = f"INSERT INTO {dbName}.{dbTable} ({columns}) VALUES ({placeholders})"
                        values = [tuple(row) for row in cdf.values]
                        cursor.executemany(query, values)
                        self.connection.commit()
                        pbar.update(len(cdf))

                except mysql.connector.Error as e:
                    self.status = DatabaseStatus.ERROR
                    self.last_error = str(e)
                    self.logger.error(f"MySQL error occurred while inserting chunk {i}: {e}")
                except Exception as e:
                    self.status = DatabaseStatus.ERROR
                    self.last_error = str(e)
                    self.logger.error(f"Unexpected error occurred while inserting chunk {i}: {e}")

        self.logger.info(f"Completed insertion of {len(dataframe)} rows into {dbName}.{dbTable}")
    @staticmethod
    def chunker_(df: pd.DataFrame, size: int):
        return (df[pos:pos + size] for pos in range(0, len(df), size))

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")
            self.connection = None
        self.status = DatabaseStatus.DISCONNECTED

    def get_status(self) -> Dict[str, str]:
        """
        Get the current status of the database connection.

        :return: A dictionary containing the status and last error (if any)
        """
        return {
            "status": self.status.value,
            "last_error": self.last_error if self.status == DatabaseStatus.ERROR else None
        }

    def check_connection(self) -> bool:
        """
        Check if the database connection is still alive and reconnect if necessary.

        :return: True if the connection is alive (or successfully reconnected), False otherwise
        """
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Failed to check/reconnect to database: {e}")
            return False

class PostGreData:
    """
    A utility class for synchronous PostgreSQL database operations with status tracking.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logger or logging.getLogger(__name__)
        self.status = DatabaseStatus.DISCONNECTED
        self.last_error = None
        self.chunksize_divider = 50

    def connect(self):
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.status = DatabaseStatus.CONNECTED
                self.logger.info("Database connection created successfully.")
            except psycopg2.Error as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"PostgreSQL error occurred while creating connection: {e}")
                raise
            except Exception as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"Unexpected error occurred while creating connection: {e}")
                raise

    @contextmanager
    def transaction(self):
        try:
            yield
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Transaction failed: {e}")
            raise

    def rollback(self):
        if self.connection:
            self.connection.rollback()
            self.logger.info("Transaction rolled back.")
    def execute_query(
            self,
            query: str,
            params: Optional[tuple] = None,
            return_type: str = 'dataframe'
    ) -> Union[pd.DataFrame, List[Dict], List[tuple]]:
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if return_type.lower() == 'dataframe':
                    result = pd.DataFrame(results, columns=columns)
                    self.logger.info(f"Executed Query: {result.head()}")
                    return result
                elif return_type.lower() == 'dict':
                    return [dict(row) for row in results]
                elif return_type.lower() == 'tuple':
                    return [tuple(row) for row in results]
                else:
                    raise ValueError(
                        f"Invalid return_type: {return_type}. Valid options are 'dataframe', 'dict', or 'tuple'.")

        except psycopg2.Error as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"PostgreSQL error occurred while executing query: {e}")
            raise
        except Exception as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Unexpected error occurred while executing query: {e}")
            raise
        finally:
            print(f"Query executed: {query}...")

    def execute_insert(self, query: str, params: tuple) -> bool:
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                self.logger.info(f"Insert query executed successfully: {query[:50]}...")
                return True
        except psycopg2.Error as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"PostgreSQL error occurred while executing insert: {e}")
            return False
        except Exception as e:
            self.status = DatabaseStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Unexpected error occurred while executing insert: {e}")
            return False

    def get_initial_book(self, ticker: str, active_effective_date: str) -> pd.DataFrame:
        query = f"""SELECT * FROM intraday.new_daily_book_format WHERE effective_date = %s AND ticker = %s;"""
        self.logger.info(f"Fetching book data for ticker {ticker} on date {active_effective_date}")
        return self.execute_query(query, (active_effective_date, ticker))

    def chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def table_exists(self, schema: str, table: str) -> bool:
        query = sql.SQL("SELECT to_regclass({})").format(
            sql.Literal(f"{schema}.{table}")
        )
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0] is not None
        except errors.Error as e:
            self.logger.error(f"Error checking table existence: {e}")
            breakpoint()
            return False

    def insert_progress(self, schema: str, table: str, dataframe: pd.DataFrame):
        if not self.connection:
            self.connect()

        full_table_name = f"{schema}.{table}"

        if not self.table_exists(schema, table):
            self.logger.error(f"Table {full_table_name} does not exist.")
            breakpoint()
            return

        chunksize = math.ceil(len(dataframe) / self.chunksize_divider)
        self.logger.info(f"Inserting DataFrame with {len(dataframe)} rows into {full_table_name} in chunks of {chunksize}")

        with tqdm(total=len(dataframe), desc="Inserting data") as pbar:
            for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
                try:
                    with self.connection.cursor() as cursor:
                        buffer = StringIO()
                        cdf.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
                        buffer.seek(0)

                        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(
                            sql.Identifier(schema, table),
                            sql.SQL(', ').join(map(sql.Identifier, cdf.columns))
                        )
                        cursor.copy_expert(copy_sql, buffer)

                        self.connection.commit()
                        pbar.update(len(cdf))

                except Exception as e:
                    self.status = DatabaseStatus.ERROR
                    self.last_error = str(e)
                    self.logger.error(f"Error occurred while inserting chunk {i}: {str(e)}")
                    self.connection.rollback()

        self.logger.info(f"Completed insertion of {len(dataframe)} rows into {full_table_name}")

    @staticmethod
    def chunker_(df: pd.DataFrame, size: int):
        return (df[pos:pos + size] for pos in range(0, len(df), size))

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")
            self.connection = None
        self.status = DatabaseStatus.DISCONNECTED

    def get_status(self) -> Dict[str, str]:
        """
        Get the current status of the database connection.

        :return: A dictionary containing the status and last error (if any)
        """
        return {
            "status": self.status.value,
            "last_error": self.last_error if self.status == DatabaseStatus.ERROR else None
        }

    def check_connection(self) -> bool:
        """
        Check if the database connection is still alive and reconnect if necessary.

        :return: True if the connection is alive (or successfully reconnected), False otherwise
        """
        try:
            if self.connection is None or self.connection.closed:

                self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Failed to check/reconnect to database: {e}")
            return False

class DigitalOceanSpaces:
    """
    A utility class for DigitalOcean Spaces operations with status tracking.
    """

    def __init__(self, region_name: str, endpoint_url: str, access_key: str, secret_key: str,
                 logger: Optional[logging.Logger] = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = None
        self.logger = logger or logging.getLogger(__name__)
        self.status = SpacesStatus.DISCONNECTED
        self.last_error = None

    def connect(self):
        if self.client is None:
            try:
                session = boto3.session.Session()
                self.client = session.client('s3',
                                             region_name=self.region_name,
                                             endpoint_url="https://nyc3.digitaloceanspaces.com",
                                             aws_access_key_id=self.access_key,
                                             aws_secret_access_key=self.secret_key)
                self.status = SpacesStatus.CONNECTED
                self.logger.info("DigitalOcean Spaces connection created successfully.")
            except Exception as e:
                self.status = SpacesStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"Unexpected error occurred while creating connection: {e}")
                raise

    def upload_to_spaces(self, local_file: str, space_name: str, spaces_file: str) -> bool:
        if not self.client:
            self.connect()

        try:
            self.client.upload_file(local_file, space_name, spaces_file)
            self.logger.info(f"Upload Successful: {spaces_file}")
            return True
        except Exception as e:
            self.status = SpacesStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"An error occurred during upload: {e}")
            return False

    def download_from_spaces(self, space_name: str, spaces_file: str, local_file: str) -> bool:
        if not self.client:
            self.connect()

        try:
            self.client.download_file(space_name, spaces_file, local_file)
            self.logger.info(f"Download Successful: {local_file}")
            return True
        except Exception as e:
            self.status = SpacesStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"An error occurred during download: {e}")
            return False

    def list_objects(self, space_name: str, prefix: str = "", max_keys: int = 1000) -> list:
        """
        List objects in a DigitalOcean Space with pagination support.

        :param space_name: Name of the Space to list objects from
        :param prefix: Prefix to filter objects (optional)
        :param max_keys: Maximum number of keys to return (default 1000, use None for all)
        :return: List of object keys
        """
        if not self.client:
            self.connect()

        all_objects = []
        continuation_token = None

        try:
            while True:
                # Prepare parameters for the list_objects_v2 call
                params = {
                    'Bucket': space_name,
                    'Prefix': prefix,
                }
                if continuation_token:
                    params['ContinuationToken'] = continuation_token
                if max_keys:
                    params['MaxKeys'] = min(max_keys - len(all_objects), 1000)  # API limit is 1000 per call

                # Make the API call
                response = self.client.list_objects_v2(**params)

                # Process the response
                objects = response.get('Contents', [])
                all_objects.extend([obj['Key'] for obj in objects])
                self.logger.debug(f"Retrieved {len(objects)} objects. Total: {len(all_objects)}")

                # Check if we need to paginate
                if not response.get('IsTruncated') or (max_keys and len(all_objects) >= max_keys):
                    break

                continuation_token = response.get('NextContinuationToken')

            self.logger.info(f"Successfully listed {len(all_objects)} objects from {space_name} with prefix '{prefix}'")
            return all_objects

        except self.client.exceptions.NoSuchBucket:
            self.status = SpacesStatus.ERROR
            self.last_error = f"Bucket {space_name} does not exist"
            self.logger.error(self.last_error)
            return []

        except self.client.exceptions.ClientError as e:
            self.status = SpacesStatus.ERROR
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            self.last_error = f"ClientError: {error_code} - {error_message}"
            self.logger.error(f"An error occurred while listing objects: {self.last_error}")
            return []

        except Exception as e:
            self.status = SpacesStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"An unexpected error occurred while listing objects: {e}")
            return []


    def get_or_initialize_metadata(self, space_name: str, metadata_key: str) -> dict:
        """
        Retrieve existing metadata or initialize new metadata.

        :param space_name: Name of the Space
        :param metadata_key: Key of the metadata file
        :return: Dictionary containing metadata
        """
        try:
            metadata_content = self.download_from_spaces_to_string(space_name, metadata_key)
            return json.loads(metadata_content)
        except Exception as e:
            self.logger.warning(f"Failed to retrieve metadata, initializing new: {e}")
            return {'last_timestamp': None, 'frames': []}

    def update_metadata(self, space_name: str, metadata_key: str, metadata: dict) -> bool:
        """
        Update metadata in DigitalOcean Spaces.

        :param space_name: Name of the Space
        :param metadata_key: Key of the metadata file
        :param metadata: Dictionary containing metadata to update
        :return: True if successful, False otherwise
        """
        try:
            self.upload_to_spaces_from_string(json.dumps(metadata), space_name, metadata_key)
            self.logger.info(f"Metadata updated successfully: {metadata_key}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update metadata: {e}")
            return False

    def download_from_spaces_to_string(self, space_name: str, spaces_file: str) -> str:
        """
        Download a file from Spaces and return its content as a string.

        :param space_name: Name of the Space
        :param spaces_file: Key of the file in Spaces
        :return: Content of the file as a string
        """
        if not self.client:
            self.connect()

        try:
            response = self.client.get_object(Bucket=space_name, Key=spaces_file)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            self.logger.error(f"An error occurred during download: {e}")
            raise

    def upload_to_spaces_from_string(self, content: str, space_name: str, spaces_file: str) -> bool:
        """
        Upload a string content to Spaces.

        :param content: String content to upload
        :param space_name: Name of the Space
        :param spaces_file: Key of the file in Spaces
        :return: True if successful, False otherwise
        """
        if not self.client:
            self.connect()

        try:
            self.client.put_object(Bucket=space_name, Key=spaces_file, Body=content.encode('utf-8'))
            self.logger.info(f"Upload Successful: {spaces_file}")
            return True
        except Exception as e:
            self.logger.error(f"An error occurred during upload: {e}")
            return False

    def get_status(self) -> dict:
        """
        Get the current status of the DigitalOcean Spaces connection.

        :return: A dictionary containing the status and last error (if any)
        """
        return {
            "status": self.status.value,
            "last_error": self.last_error if self.status == SpacesStatus.ERROR else None
        }

    def check_connection(self) -> bool:
        """
        Check if the DigitalOcean Spaces connection is still alive and reconnect if necessary.

        :return: True if the connection is alive (or successfully reconnected), False otherwise
        """
        try:
            if self.client is None:
                self.connect()
            # Perform a simple operation to check if the connection is alive
            self.client.list_buckets()
            return True
        except Exception as e:
            self.logger.error(f"Failed to check/reconnect to DigitalOcean Spaces: {e}")
            return False

    def close(self):
        self.client = None
        self.status = SpacesStatus.DISCONNECTED
        self.logger.info("DigitalOcean Spaces connection closed.")