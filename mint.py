# connections
from mintapi import Mint
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config import sql_addr
import os

# data manipulation
import pandas as pd
from pandas import json_normalize
from datetime import datetime
import json
import time
import pytz

# environment
load_dotenv()
MINT_USER = os.getenv('MINT_USER')
MINT_PASS = os.getenv('MINT_PASS')
MINT_TOKEN = os.getenv('MINT_TOKEN')

# get time
now = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
now_str = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d_%H%M%S')

# connect
def connect_to_mint():
    mint = Mint(MINT_USER,
                MINT_PASS,
                mfa_method='soft-token',
                mfa_token=MINT_TOKEN,
                headless=True,
                wait_for_sync=True
                )
    return mint

# get transactions
def get_transactions(mint, now):
    
    # get the raw data
    trans_raw = mint.get_transaction_data()

    # pick specific columns and keep in this order
    cols_to_keep = ['id', 'date', 'amount', 'transactionType', 'description'
        'category_name', 'category_parentName', 'accountRef_name', 'tagData_name'
        'notes', 'isReviewed', 'lastUpdatedDate', 'parentId'
        'fiData_amount', 'fiData_date', 'fiData_description'
        'accountRef_id', 'accountRef_type', 'category_id', 'category_parentId', 'insert_ts']

    # removes inner level "tags" from "tagData" column
    for transaction in trans_raw:
        if 'tagData' in transaction:
            tag_data = transaction['tagData']
            if tag_data and 'tags' in tag_data:
                transaction['tagData'] = tag_data['tags'][0] if tag_data['tags'] else {}

    # normalize json and send to dataframe
    trans_json = json_normalize(trans_raw)
    df = pd.DataFrame(trans_json)

    # confirm columns
    df.columns = df.columns.str.replace('[.\s]', '_', regex=True)
    df['insert_ts'] = now
    df = df[cols_to_keep]

    return df

# get accounts
def get_accounts(mint, now):

    # get the raw data
    accnt_raw = mint.get_account_data()

    # send to dataframe
    df = pd.DataFrame(accnt_raw)

    # clean up columns
    df['id'] = df['id'].str.split('_').str[1]
    df['type'] = df.apply(lambda row: row['bankAccountType'] if pd.notnull(row['bankAccountType']) else ('CREDIT' if row['type'] == 'CreditAccount' else row['type']), axis=1)
    df = df.rename(columns={'id': 'account_id', 
                            'name': 'account_name', 
                            'currentBalance': 'account_balance',
                            'type': 'account_type'})
    df['insert_ts'] = now
    df.columns = df.columns.str.replace('[.\s]', '_', regex=True)

    return df

# connect
mint = connect_to_mint()
engine = create_engine(sql_addr)

# get transactions
trans_df = get_transactions(mint, now)
trans_df.to_csv(f'files/trans_df_{now_str}.csv', index=False)
try:
    # get columns from table
    trans_df.to_sql('transactions_history', con=engine, if_exists='append', index=False)
    print(f"Success sending transactions to sql")
except Exception as e:
    print(f"Error when trying to send transactions to sql: {e}")

# get accounts
accnt_df = get_accounts(mint, now)
accnt_df.to_csv(f'files/accnt_df_{now_str}.csv', index=False)
try:    
    accnt_columns = pd.read_sql_query("SELECT * FROM accounts_history LIMIT 0", engine).columns
    accnt_filtered = accnt_df[accnt_columns]
    accnt_filtered.to_sql('accounts_history', con=engine, if_exists='append', index=False)
    print(f"Success sending accounts to sql")
except Exception as e:
    print(f"Error when trying to send accounts to sql: {e}")

# close connection
mint.close()