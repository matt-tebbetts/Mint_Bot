import pandas as pd
import os
import json
from dotenv import load_dotenv
from lunchable import LunchMoney
from typing import Any, Dict, List

# Load environment variables
load_dotenv()
LUNCHMONEY_TOKEN = os.getenv("LUNCHMONEY_TOKEN")

# Connect to LunchMoney
lunch = LunchMoney(access_token=LUNCHMONEY_TOKEN)

# Fetch transactions
transactions_raw = lunch.get_transactions(start_date='2021-01-01', end_date='2024-02-11')

# Convert transactions to a list of dictionaries
transaction_as_dict_list = [t.model_dump() for t in transactions_raw]

# Create a DataFrame from the list of dictionaries
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

# Define CSV file path
csv_file_path = 'transactions.csv'

# Export DataFrame to CSV, handling non-ASCII characters
df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
