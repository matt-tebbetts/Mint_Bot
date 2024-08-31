import os
from dotenv import load_dotenv
from lunchable import LunchMoney
from datetime import date
import pandas as pd
import json
from config import sql_addr
import config
import sqlalchemy

# get current date using datetime module
today = date.today().strftime("%Y-%m-%d")

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

def flatten_column(df, column_name):
    if column_name in df.columns:
        # Assume column_name is already a dict or similar structure that can be directly normalized
        # Directly normalize column_name without using json.loads
        column_df = pd.json_normalize(df[column_name])
        
        # Rename columns to prevent potential name clashes
        column_df.columns = [column_name + '_' + str(col) for col in column_df.columns]
        
        # Drop the original column_name column from df
        df.drop(columns=[column_name], inplace=True)
        
        # Concatenate the original DataFrame with the flattened column DataFrame
        df = pd.concat([df, column_df], axis=1)
    
    return df

def get_transactions(token, start_date='2020-01-01'):
    print("Fetching transactions...")
    try:
        # connect
        lunch = LunchMoney(access_token=token)
        raw = lunch.get_transactions(start_date=start_date, end_date=today)
        transaction_as_dict_list = [t.model_dump() for t in raw]
        print("Transactions fetched successfully.")
        return transaction_as_dict_list
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return []

def save_as_json(data, json_file_path):
    print("Saving data as JSON...")
    try:
        with open(json_file_path, 'w') as f:
            json.dump(data, f, cls=DateEncoder)
        print("Data saved to JSON successfully.")
    except Exception as e:
        print(f"Error saving data as JSON: {e}")

def convert_to_dataframe(data):
    print("Converting data to DataFrame...")
    try:
        df = pd.DataFrame(data)
        print("Data converted to DataFrame successfully.")
        return df
    except Exception as e:
        print(f"Error converting data to DataFrame: {e}")
        return pd.DataFrame()

def save_to_csv(df, csv_file_path):
    print("Saving DataFrame to CSV...")
    try:
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
        print("DataFrame saved to CSV successfully.")
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")

# setup
load_dotenv()
token = os.getenv("LUNCHMONEY_TOKEN")
json_file_path = 'files/lunchmoney/transactions.json'
csv_file_path = 'files/lunchmoney/transactions.csv'

# Task 1: Get the transactions and save as a JSON
transactions = get_transactions(token)
save_as_json(transactions, json_file_path)

# Task 2: Convert transactions to DataFrame and save as CSV
df = convert_to_dataframe(transactions)
df = flatten_column(df, 'plaid_metadata')
df = flatten_column(df, 'tags')
save_to_csv(df, csv_file_path)

# Task 3: Send to SQL database using sql_addr from config.py
engine = sqlalchemy.create_engine(config.sql_addr)
df.to_sql('lm_transactions', engine, if_exists='replace', index=False)