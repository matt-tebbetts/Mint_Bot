import os
from dotenv import load_dotenv
from lunchmoney import LunchMoney
from datetime import date
import pandas as pd
from config import sql_addr
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import NoSuchTableError

# get current date using datetime module
today = date.today().strftime("%Y-%m-%d")

# Create a SQL Alchemy engine
engine = create_engine("sql_addr")

def get_table_schema(table_name: str) -> pd.DataFrame:
    """Fetch the schema of a table from the database."""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return pd.DataFrame.from_records(columns)
    except NoSuchTableError:
        return pd.DataFrame()

def df_to_sql(df: pd.DataFrame, table_name: str):
    """Insert data from a DataFrame into a SQL table, handling schema discrepancies."""
    # Get the current schema of the table
    schema_df = get_table_schema(table_name)
    
    if schema_df.empty:
        # If the table does not exist, create it
        df.to_sql(table_name, engine, index=False, if_exists='fail')
    else:
        # If the table exists, check for schema discrepancies
        common_cols = set(df.columns).intersection(set(schema_df['name']))
        if len(common_cols) < len(df.columns):
            print(f"Warning: DataFrame contains columns not in table {table_name}. Only common columns will be inserted.")
        
        # Insert data from the DataFrame into the table, only for common columns
        df[list(common_cols)].to_sql(table_name, engine, index=False, if_exists='append')

# Call the function with your DataFrame and table name
df_to_sql(df, 'your_table_name')

def get_transactions(token, start_date='2020-01-01'):

    # connect
    lunch = LunchMoney(access_token=token)
    raw = lunch.get_transactions(start_date=start_date, end_date=today)
    transaction_as_dict_list = [t.model_dump() for t in raw]
    df = pd.DataFrame(transaction_as_dict_list)

    # Check if 'plaid_metadata' column exists and needs to be flattened
    if 'plaid_metadata' in df.columns:
        # Assume 'plaid_metadata' is already a dict or similar structure that can be directly normalized
        # Directly normalize 'plaid_metadata' without using json.loads
        plaid_metadata_df = pd.json_normalize(df['plaid_metadata'])
        
        # Rename columns to prevent potential name clashes
        plaid_metadata_df.columns = ['plaid_' + col for col in plaid_metadata_df.columns]
        
        # Drop the original 'plaid_metadata' column from df
        df.drop(columns=['plaid_metadata'], inplace=True)
        
        # Concatenate the original DataFrame with the flattened 'plaid_metadata' DataFrame
        df = pd.concat([df, plaid_metadata_df], axis=1)

    return df

def save_to_csv(df, csv_file_path):
    df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

# setup
load_dotenv()
token = os.getenv("LUNCHMONEY_TOKEN")
csv_file_path = 'files/transactions.csv'

# get transactions
df = get_transactions(token)

# save to csv
save_to_csv(df, csv_file_path)

