from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import time
from utilities.db_utils import *
from config.config import *


db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')

@task(name="Verify Table Synchronization",
      retries=6,
      retry_delay_seconds=600,
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
def verify_table_sync():
    """Verify if OC and EOD tables are synchronized."""
    if not od.tables_are_synced():
        raise Exception("OC and EOD not Synchronized")
    return True


@task(name="Get Dates to Process")
def get_dates_to_process():
    """Determine which dates need to be processed."""
    oc_dates = db.execute_query('SELECT DISTINCT(quote_date) FROM landing.OC')
    as_of_dates = db.execute_query('SELECT DISTINCT(as_of_date) FROM intraday.new_daily_book_format')

    merged_df = pd.merge(oc_dates, as_of_dates, left_on='quote_date', right_on='as_of_date', how='left')
    merged_df['quote_date'] = pd.to_datetime(merged_df['quote_date'])
    filtered_df = merged_df[merged_df['quote_date'] > '2024-01-01']
    return filtered_df[filtered_df['as_of_date'].isna()]['quote_date'].dt.date.values


@task(name="Generate Book", retries=3, retry_delay_seconds=300)
def generate_book(ticker: str, date: str):
    """Generate a book for a specific ticker and date."""
    return update_mm_book_with_latest_data(date)


@task(name="Check Existing Entry")
def check_existing_entry(date: str, ticker: str):
    """Check if an entry already exists for the given date and ticker."""
    return date_isin_mm_books(date, ticker.replace('^', ''))


@task(name="Delete Existing Entry")
def delete_existing_entry(ticker: str, date: str):
    """Delete existing entry for the given ticker and date."""
    return delete_entries(ticker.replace('^', ''), date)


@task(name="Insert Book to Table")
def insert_book_to_table(df_book: pd.DataFrame):
    """Insert the generated book into the database table."""
    db.insert_progress(df_book, 'intraday', 'new_daily_book_format')


@flow(name="Generate MM Books Flow",
      description="Daily flow to generate and update MM books, running at 2 AM.")
def generate_mm_books_flow(override: bool = False):
    ticker = '^SPX'

    # Verify table synchronization
    sync_status = verify_table_sync()

    if sync_status:
        dates_to_process = get_dates_to_process()

        for date in reversed(dates_to_process):
            print(f'Processing quote_date: {date}')

            existing_entry = check_existing_entry(str(date), ticker)

            if existing_entry and override:
                print(f'Overriding entry for {date}')
                delete_existing_entry(ticker, str(date))
            elif existing_entry and not override:
                print(f"{date} already in table. No override.")
                continue

            df_book = generate_book(ticker, str(date))

            if df_book is not None:
                insert_book_to_table(df_book)
                print(f"Successfully processed and inserted data for {date}")
            else:
                print(f"Failed to generate book for {date}")


if __name__ == "__main__":
    generate_mm_books_flow(override=True)