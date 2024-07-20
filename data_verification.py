import pandas as pd
import numpy as np

INTRADAY_REQUIRED_COLUMNS = [
    'trade_datetime', 'ticker', 'security_type', 'option_symbol', 'expiration_date',
    'strike_price', 'call_put_flag', 'days_to_expire', 'series_type', 'previous_close'
]

VALID_SECURITY_TYPES = [1, 2]
VALID_CALL_PUT_FLAGS = ['C', 'P']
VALID_SERIES_TYPES = ['A', 'B']

def verify_data(df):
    missing_columns = set(INTRADAY_REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required column(s): {missing_columns}")

    expected_types = {
        'trade_datetime': 'object',
        'ticker': 'object',
        'security_type': 'int64',
        'option_symbol': 'object',
        'expiration_date': 'object',
        'strike_price': 'float64',
        'call_put_flag': 'object',
        'days_to_expire': 'int64',
        'series_type': 'object',
        'previous_close': 'float64'
    }

    for col, expected_type in expected_types.items():
        if df[col].dtype != expected_type:
            raise ValueError(f"Column {col} has incorrect data type. Expected {expected_type}, got {df[col].dtype}")

    if not df['security_type'].isin(VALID_SECURITY_TYPES).all():
        raise ValueError("Invalid security_type values found")

    if not df['call_put_flag'].isin(VALID_CALL_PUT_FLAGS).all():
        raise ValueError("Invalid call_put_flag values found")

    if not df['series_type'].isin(VALID_SERIES_TYPES).all():
        raise ValueError("Invalid series_type values found")

    volume_qty_columns = [col for col in df.columns if (col.endswith('_qty') or col.endswith('_vol')) and not col.startswith('total_')]
    for col in volume_qty_columns:
        if not ((df[col] >= 0) & df[col].apply(lambda x: isinstance(x, (int, np.integer)) or (isinstance(x, float) and x.is_integer()))).all():
            raise ValueError(f"Column {col} contains negative or non-integer values")

    if not (df['strike_price'] > 0).all():
        raise ValueError("strike_price should be positive")

    if not (df['days_to_expire'] >= 0).all():
        raise ValueError("days_to_expire should be non-negative")

    return df

def process_greek(greek_name, poly_data, book):
    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'))
    update_col = f"{greek_name}_update"
    if update_col in book.columns:
        book[greek_name] = book[update_col].combine_first(book[greek_name])
        book.drop(columns=[update_col, "time_stamp_update"], inplace=True)
    return book

# Add other data verification and processing functions as needed
