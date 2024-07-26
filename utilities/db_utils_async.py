import math
from typing import List, Optional, Dict
import aiomysql
import pandas as pd
import logging
from enum import Enum

from tqdm import tqdm


class DatabaseStatus(Enum):
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
        self.status = DatabaseStatus.DISCONNECTED
        self.last_error = None
        self.chunksize_divider = 50

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
                    maxsize=10
                )
                self.status = DatabaseStatus.CONNECTED
                self.logger.info("Database connection created successfully.")
            except aiomysql.Error as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"MySQL error occurred while creating connection: {e}")
                raise
            except Exception as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)
                self.logger.error(f"Unexpected error occurred while creating connection: {e}")
                raise

    async def execute_query(self, query: str, params: tuple = None) -> List[dict]:
        """
        Execute a SQL query and return the results.

        :param query: SQL query string to execute
        :param params: Optional tuple of parameters to use with the query
        :return: List of dictionaries representing the query results

        Exceptions:
        - aiomysql.Error: If there's an error executing the query.
        - Exception: For any other unexpected errors during query execution.
        """
        #TODO: Should return a DataFrame
        if not self.pool:
            await self.create_pool()

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    results = await cursor.fetchall()
                    self.logger.info(f"Query executed successfully: {query[:50]}...")

                    #results_df = pd.DataFrame(results)

                    return results
        except aiomysql.Error as e:
            self.logger.error(f"MySQL error occurred while executing query: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while executing query: {e}")
            raise

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

    async def fetch_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a pandas DataFrame.

        :param query: SQL query string to execute
        :param params: Optional tuple of parameters to use with the query
        :return: pandas DataFrame containing the query results

        Exceptions:
        - Same as execute_query method.
        """
        results = await self.execute_query(query, params)
        df = pd.DataFrame(results) if results else pd.DataFrame()
        self.logger.info(f"Query results fetched as DataFrame with shape {df.shape}")
        return df

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
        return await self.fetch_dataframe(query, (active_effective_date, ticker))

    @staticmethod
    def chunker(seq, size):
        """
        Split a sequence into chunks of specified size.

        :param seq: Sequence to split
        :param size: Size of each chunk
        :return: Generator yielding sequence chunks
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    async def insert_progress(self, dbname: str, dbtable: str, dataframe: pd.DataFrame):
        """
        Insert a large DataFrame into the database in chunks, with progress tracking.

        :param dbname: Database name
        :param dbtable: Table name
        :param dataframe: pandas DataFrame to insert

        This method splits the DataFrame into chunks and inserts them separately to avoid
        overwhelming the database with a single large insert operation.

        Exceptions:
        - aiomysql.Error: If there's an error during the insert operation.
        - Exception: For any other unexpected errors during the insert process.
        """
        if not self.pool:
            await self.create_pool()

        chunksize = math.ceil(len(dataframe) / self.chunksize_divider)
        self.logger.info(
            f"Inserting DataFrame with {len(dataframe)} rows into {dbname}.{dbtable} in chunks of {chunksize}")

        with tqdm(total=len(dataframe), desc="Inserting data") as pbar:
            for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
                try:

                    #inserted = cdf.to_sql()
                    async with self.pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            placeholders = ', '.join(['%s'] * len(cdf.columns))
                            columns = ', '.join(cdf.columns)
                            query = f"INSERT INTO {dbname}.{dbtable} ({columns}) VALUES ({placeholders})"
                            values = [tuple(row) for row in cdf.values]
                            await cursor.executemany(query, values)
                            await conn.commit()
                            pbar.update(len(cdf))

                except aiomysql.Error as e:
                    self.status = DatabaseStatus.ERROR
                    self.last_error = str(e)
                    self.logger.error(f"MySQL error occurred while inserting chunk {i}: {e}")
                except Exception as e:
                    self.status = DatabaseStatus.ERROR
                    self.last_error = str(e)
                    self.logger.error(f"Unexpected error occurred while inserting chunk {i}: {e}")

        self.logger.info(f"Completed insertion of {len(dataframe)} rows into {dbname}.{dbtable}")

    async def get_status(self) -> Dict[str, str]:
        """
        Get the current status of the database connection.

        :return: A dictionary containing the status and last error (if any)
        """
        if self.pool is None:
            self.status = DatabaseStatus.DISCONNECTED
        elif self.pool.closed:
            self.status = DatabaseStatus.DISCONNECTED
        else:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                self.status = DatabaseStatus.CONNECTED
            except Exception as e:
                self.status = DatabaseStatus.ERROR
                self.last_error = str(e)

        return {
            "status": self.status.value,
            "last_error": self.last_error if self.status == DatabaseStatus.ERROR else None
        }


