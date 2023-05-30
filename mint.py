# connections
from mintapi import Mint
from mintapi import DateFilter
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

# get columns from sql
def get_sql_cols(engine, table_name):
    cols_to_keep = engine.execute(f"SELECT * FROM {table_name} LIMIT 0").keys()
    return cols_to_keep

# set renaming dictionary
def find_renaming_dict(engine, table_name):
    with open('dict_trans.csv', 'r') as csvfile:
        cols_to_rename = dict(line.strip().split(',') for line in csvfile.readlines()[1:])
    return cols_to_rename

def rename_keys(mapping, rename_dict):
    return {rename_dict.get(key, key): value for key, value in mapping.items()}

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
def get_transactions(mint, engine, now):
    
    # set up dataframe to match format of sql table
    table_name = 'transactions_history'
    cols_to_keep = get_sql_cols(engine, table_name)
    df = pd.DataFrame(columns=cols_to_keep)

    # set time range and get the data from mint
    time_range = DateFilter.Options.LAST_3_MONTHS
    trans_raw = mint.get_transaction_data(time_range)

    # removes inner level of one json column
    for transaction in trans_raw:
        if 'tagData' in transaction:
            tag_data = transaction['tagData']
            if tag_data and 'tags' in tag_data:
                transaction['tagData'] = tag_data['tags'][0] if tag_data['tags'] else {}
    
    # normalize json and create dataframe
    trans_df = json_normalize(trans_raw)
    cols_to_rename = find_renaming_dict(engine, table_name)
    trans_df = trans_df.rename(columns=cols_to_rename)

    # get data from trans_json_renamed and append to df
    df = pd.concat([df, trans_df], ignore_index=True, join='left')
    df = df.replace({np.nan: None})

    # clean up some columns
    df['insert_ts'] = now
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # clean up the ids
    df['trans_id'] = df['trans_id'].apply(lambda x: x.split('_', 1)[-1])
    for col in ['trans_id', 'category_id', 'category_parent_id']:
        df[col] = df[col].str.split('_').str[0]

    return df

# get accounts
def get_accounts(mint, engine, now):

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
trans_df = get_transactions(mint, engine, now)
trans_df.to_csv(f'files/trans_df_{now_str}.csv', index=False)
try:
    trans_df.to_sql('transactions_history', con=engine, if_exists='append', index=False)
    print(f"Success sending transactions to sql")
except Exception as e:
    print(f"Error when trying to send transactions to sql: {e}")

# accounts
accnt_df = get_accounts(mint, engine, now)
accnt_df.to_csv(f'files/accnt_df_{now_str}.csv', index=False)

try:    
    accnt_df.to_sql('accounts_history', con=engine, if_exists='append', index=False)
    print(f"Success sending accounts to sql")
except Exception as e:
    print(f"Error when trying to send accounts to sql: {e}")

# close connection
mint.close()