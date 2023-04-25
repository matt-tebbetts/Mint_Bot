# data manipulation
import pandas as pd
from datetime import datetime
import json
import re

# connections
from mintapi import Mint
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config import sql_addr
import os
import sys
from selenium import webdriver

# prevent printing of chromedriver messages
"""
# ?
chrome_log = 'files/chromedriver_output.txt'
sys.stdout = open(chrome_log, 'a+', buffering=1)
sys.stderr = open(os.devnull, 'w')

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-extensions')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument('--remote-debugging-port=9222')
options.add_argument('--silent')
options.add_experimental_option('excludeSwitches', ['enable-logging'])

with open(os.devnull, 'w') as devnull:
    driver = webdriver.Chrome(options=options, service_log_path=devnull)
"""

# get env variables
load_dotenv()
MINT_USER = os.getenv('MINT_USER')
MINT_PASS = os.getenv('MINT_PASS')
MINT_TOKEN = os.getenv('MINT_TOKEN')

# start mint
def get_mint_data():
    mint = Mint(MINT_USER,
                MINT_PASS,
                mfa_method='soft-token',
                mfa_token=MINT_TOKEN,
                headless=True
                )

    return mint

# clean transactions dataframe
def clean_transactions(df):

    df = df.copy()

    # pick columns
    cols_to_keep = ['type', 'id', 'date', 'amount', 'transactionType', 'status', 'description', 'accountRef', 'category', 'tagData']
    cols_to_json = ['accountRef', 'category', 'tagData']
    cols_to_drop = cols_to_json
    df = df[cols_to_keep]

    # fix this strange column
    df.loc[:, 'tagData'] = df.loc[:, 'tagData'].fillna("{'tags': [{'id': 'nan', 'name': 'nan', 'hiddenFromPlanningAndTrends': 'nan'}]}")
    df.loc[:, 'tagData'] = df.loc[:, 'tagData'].apply(lambda x: str(x).replace("{'tags': [", ""))
    df.loc[:, 'tagData'] = df.loc[:, 'tagData'].apply(lambda x: str(x).replace("]}", ""))

    # normalize json columns
    for col in cols_to_json:

        # clean it up to make proper json
        df.loc[:, col] = df.loc[:, col].apply(lambda x: str(x).replace("True", '"True"').replace("False", '"False"'))
        df.loc[:, col] = df.loc[:, col].apply(lambda x: re.sub("'", '"', x))

        # escape unescaped double quotes within string values (like Dick's Sporting Goods)
        df.loc[:, col] = df.loc[:, col].apply(lambda x: re.sub(r'(?<!\\)"', r'\"', x) if isinstance(x, str) else x)
        df.loc[:, col] = df.loc[:, col].apply(lambda x: (print(f"Error decoding: {x}") or json.loads(x)) if isinstance(x, str) else x)

        # Create new columns from each key
        for key in df[col][0].keys():
            new_column = f"{col}_{key}"
            df.loc[:, new_column] = df.loc[:, col].apply(lambda x: x[key])

    # correct the ids
    cols_to_split = ['id', 'category_id', 'category_parentId', 'tagData_id']
    for col in cols_to_split:
        df.loc[:, col] = df.loc[:, col].apply(lambda x: x.split('_')[1] if '_' in x else x)

    # drop columns
    cols_to_drop += ['type', 'tagData_hiddenFromPlanningAndTrends', 'accountRef_hiddenFromPlanningAndTrends']
    df = df.drop(columns=cols_to_drop)
    
    # add timestamp
    df['py_insert_ts'] = df.apply(lambda x: datetime.now(), axis=1)

    # return cleaned dataframe
    return df

# returns clean dataframe of accounts
def clean_accounts(df):

    # conditions to keep
    cond_1 = (df['type'] == 'CreditAccount') | (df['type'] == 'BankAccount')
    cond_2 = df['isVisible'] == True
    cond_3 = df['accountStatus'] == 'ACTIVE'

    # clean id
    df['id'] = df['id'].str.split('_').str[1]

    # clean type
    df['type'] = df.apply(lambda row: row['bankAccountType'] if pd.notnull(row['bankAccountType']) else ('CREDIT' if row['type'] == 'CreditAccount' else row['type']), axis=1)

    # rename cols
    df = df.rename(columns={'id': 'account_id', 
                            'name': 'account_name', 
                            'currentBalance': 'account_balance',
                            'type': 'account_type'})

    # columns to keep
    keep_cols = ['account_id', 'account_type', 'account_name', 'account_balance']

    # apply conditions
    cleaned_df = df[cond_1 & cond_2 & cond_3][keep_cols]

    # add timestamp
    cleaned_df['py_insert_ts'] = datetime.now()

    return cleaned_df

# get all
def get_data(mint):

    mint = get_mint_data()

    # create sql connection
    engine = create_engine(sql_addr)

    # get transactions
    transactions = clean_transactions(pd.DataFrame(mint.get_transaction_data()))
    transactions.to_csv('files/transactions.csv', index=False)
    transactions.to_sql('transactions_history', con=engine, if_exists='append', index=False)
    
    # get accounts
    accounts = clean_accounts(pd.DataFrame(mint.get_account_data()))
    accounts.to_csv('files/accounts.csv', index=False)
    accounts.to_sql('accounts_history', con=engine, if_exists='append', index=False)

# run
mint = get_mint_data()
get_data(mint)

