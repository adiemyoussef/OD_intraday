import math
from typing import Optional, Union, List, Dict
import pandas as pd
import logging
import asyncio
import aiomysql
from prefect import get_run_logger

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
        self.logger = logger or get_run_logger()

    async def create_pool(self):
        """
        Create a connection pool to the database.

        This method should be called before any other database operations.
        It creates a pool of connections that can be used for executing queries.

        Exceptions:
        - aiomysql.Error: If there's an error connecting to the database.
        - Exception: For any other unexpected errors during pool creation.
        """
        if self.pool is None or self.pool.closed:
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
                    maxsize=10
                )
                self.logger.info("Database connection pool created successfully.")
            except aiomysql.Error as e:
                self.logger.error(f"MySQL error occurred while creating pool: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error occurred while creating pool: {e}")
                raise
        return self.pool

    def execute_query_sync(self, query, params=None):
        self.connect()
        try:
            with self.connection.cursor(dictionary=True) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                df = pd.DataFrame(results)
                print(df.head())
                return df
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
        finally:
            self.logger.info(f"Query executed: {query[:50]}...")
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Synchronous version of execute_query"""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._execute_query_async(query, params))

    async def _execute_query_async(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Asynchronous version of execute_query"""
        pool = await self.create_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    results = await cursor.fetchall()
                    columns = [column[0] for column in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
                    print(df.head())
                    return df
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
        finally:
            self.logger.info(f"Query executed: {query[:50]}...")
    async def execute_query_backup(
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
        pool = await self.create_pool()
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    results = await cursor.fetchall()

                    columns = [column[0] for column in cursor.description]

                    if return_type.lower() == 'dataframe':
                        df = pd.DataFrame(results, columns=columns)
                        print(df.head())
                        return df
                    elif return_type.lower() == 'dict':
                        return [dict(zip(columns, row)) for row in results]
                    elif return_type.lower() == 'tuple':
                        return results
                    else:
                        raise ValueError(f"Invalid return_type: {return_type}. Valid options are 'dataframe', 'dict', or 'tuple'.")
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
        pool = await self.create_pool()
        try:
            async with pool.acquire() as conn:
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
        query = """SELECT * FROM intraday.new_daily_book_format WHERE effective_date = %s AND ticker = %s;"""
        self.logger.info(f"Fetching book data for ticker {ticker} on date {active_effective_date}")
        return await self.execute_query(query, (active_effective_date, ticker))

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
        pool = await self.create_pool()
        chunksize = math.ceil(len(dataframe) / 50)
        self.logger.info(f"Inserting DataFrame with {len(dataframe)} rows into {dbName}.{dbTable} in chunks of {chunksize}")

        for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
            try:
                async with pool.acquire() as conn:
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

    async def close_pool(self):
        """
        Close the database connection pool.

        This method should be called when the database operations are finished to release resources.
        """
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("Database connection pool closed.")

    async def __aenter__(self):
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_pool()


import mysql.connector
import pandas as pd
from prefect import get_run_logger
from mysql.connector import Error as MySQLError
class DatabaseUtilities:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = get_run_logger()

    def connect(self):
        if not self.connection:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.logger.info("Database connection established successfully.")

    def execute_query(self, query, params=None):
        self.connect()
        try:
            with self.connection.cursor(dictionary=True) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                df = pd.DataFrame(results)
                print(df.head())
                return df
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
        finally:
            self.logger.info(f"Query executed: {query[:50]}...")

    # ... (previous methods remain the same)

    def insert_progress(self, dbName: str, dbTable: str, dataframe: pd.DataFrame):
        """
        Insert a large DataFrame into the database in chunks.

        :param dbName: Database name
        :param dbTable: Table name
        :param dataframe: pandas DataFrame to insert

        This method splits the DataFrame into chunks and inserts them separately to avoid
        overwhelming the database with a single large insert operation.

        Exceptions:
        - MySQLError: If there's an error during the insert operation.
        - Exception: For any other unexpected errors during the insert process.
        """
        self.connect()
        chunksize = math.ceil(len(dataframe) / 50)
        self.logger.info(f"Inserting DataFrame with {len(dataframe)} rows into {dbName}.{dbTable} in chunks of {chunksize}")

        for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
            try:
                cursor = self.connection.cursor()
                placeholders = ', '.join(['%s'] * len(cdf.columns))
                columns = ', '.join(cdf.columns)
                sql = f"INSERT INTO {dbName}.{dbTable} ({columns}) VALUES ({placeholders})"
                values = [tuple(row) for row in cdf.values]
                cursor.executemany(sql, values)
                self.connection.commit()
                self.logger.info(f"Inserted chunk {i + 1} ({len(cdf)} rows)")
            except MySQLError as e:
                self.logger.error(f"MySQL error occurred while inserting chunk {i}: {e}")
                self.connection.rollback()
            except Exception as e:
                self.logger.error(f"Unexpected error occurred while inserting chunk {i}: {e}")
                self.connection.rollback()
            finally:
                cursor.close()

    @staticmethod
    def chunker(df: pd.DataFrame, size: int):
        """
        Split a DataFrame into chunks of specified size.

        :param df: pandas DataFrame to split
        :param size: Size of each chunk
        :return: Generator yielding DataFrame chunks
        """
        return (df[pos:pos + size] for pos in range(0, len(df), size))

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")