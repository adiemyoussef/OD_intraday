import math
from typing import Optional, Union, List, Dict
import aiomysql
import pandas as pd
import logging
import asyncio
import mysql.connector
from enum import Enum
from tqdm import tqdm
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




class DatabaseStatus(Enum):
    DISCONNECTED = "Disconnected"
    CONNECTED = "Connected"
    ERROR = "Error"

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
            params: Optional[tuple] = None,
            return_type: str = 'dataframe'
    ) -> Union[pd.DataFrame, List[Dict], List[tuple]]:
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor(dictionary=True) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                columns = [column[0] for column in cursor.description]

                if return_type.lower() == 'dataframe':
                    result = pd.DataFrame(results)
                    self.logger.debug(f"Executed Query: {result.head()}")
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
            self.logger.debug(f"Query executed: {query}...")

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