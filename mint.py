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
import numpy as np
import csv
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
    
    # set columns
    column_mapping = {}
    with open('dict_trans.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            key, value = row
            column_mapping[key] = value
    cols_to_keep = list(column_mapping.keys())

    # get the raw data
    trans_raw = mint.get_transaction_data()

    # removes inner level "tags" from "tagData" column
    for transaction in trans_raw:
        if 'tagData' in transaction:
            tag_data = transaction['tagData']
            if tag_data and 'tags' in tag_data:
                transaction['tagData'] = tag_data['tags'][0] if tag_data['tags'] else {}

    # normalize json and send to dataframe
    trans_json = json_normalize(trans_raw)
    df = pd.DataFrame(trans_json)

    # print df.columns
    print(f"default columns are: {df.columns}")

    # select certain columns
    df.columns = df.columns.str.replace('[.\s]', '_', regex=True)
    df['insert_ts'] = now
    df = df[cols_to_keep]
    df = df.rename(columns=column_mapping)

    # clean up some columns
    df['trans_id'] = df['trans_id'].str.split('_').str.get(1).str.split('_').str.get(0)
    df['category_id'] = df['category_id'].str.split('_').str.get(0)
    df['category_parent_id'] = df['category_parent_id'].str.split('_').str.get(0)
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

# get accounts
def get_accounts(mint, now):

    # set columns
    column_mapping = {}
    with open('dict_accnt.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            key, value = row
            column_mapping[key] = value
    cols_to_keep = list(column_mapping.keys())

    # get the raw data
    accnt_raw = mint.get_account_data()

    # send to dataframe
    df = pd.DataFrame(accnt_raw)

    # print df.columns
    print(f"default columns are: {df.columns}")

    df = df[df['isActive'] == 1]
    df['insert_ts'] = now
    
    # if bankAccountType is not null, overwrite the "type" column
    df['type'] = np.where(df['bankAccountType'].notnull(), df['bankAccountType'], df['type'])

    # keep only some columns, then rename them
    df = df[cols_to_keep]
    df = df.rename(columns=column_mapping)
    df['account_id'] = df['account_id'].str.split('_').str[1]

    return df

# connect
mint = connect_to_mint()
engine = create_engine(sql_addr)

# transactions
trans_df = get_transactions(mint, now)
trans_df.to_csv(f'files/trans_df_{now_str}.csv', index=False)
try:
    trans_df.to_sql('transactions_history', con=engine, if_exists='append', index=False)
    print(f"Success sending transactions to sql")
except Exception as e:
    print(f"Error when trying to send transactions to sql: {e}")

# accounts
accnt_df = get_accounts(mint, now)
accnt_df.to_csv(f'files/accnt_df_{now_str}.csv', index=False)

try:    
    accnt_df.to_sql('accounts_history', con=engine, if_exists='append', index=False)
    print(f"Success sending accounts to sql")
except Exception as e:
    print(f"Error when trying to send accounts to sql: {e}")

# close connection
mint.close()