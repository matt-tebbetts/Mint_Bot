# import packages
import pandas as pd
from datetime import datetime
import re
import json
import os
from dotenv import load_dotenv
from mintapi import Mint
from sqlalchemy import create_engine
from config import sql_addr

# get env variables
load_dotenv()
MINT_USER = os.getenv('MINT_USER')
MINT_PASS = os.getenv('MINT_PASS')
MINT_TOKEN = os.getenv('MINT_TOKEN')

# get data
def get_transactions_and_accounts(mint):

    transactions = mint.get_transaction_data()
    accounts = mint.get_account_data()

    # Print variable types
    print(f"transactions variable is type {type(transactions)}")
    print(f"accounts variable is type {type(accounts)}")

    # Convert to dataframe
    transactions_df = pd.DataFrame(transactions)
    accounts_df = pd.DataFrame(accounts)

    return transactions_df, accounts_df

# returns clean dataframe of transactions
def clean_transactions(df):

    # pick columns
    cols_to_keep = ['type', 'id', 'date', 'amount', 'transactionType', 'status', 'description', 'accountRef', 'category', 'tagData']
    cols_to_json = ['accountRef', 'category', 'tagData']
    cols_to_drop = cols_to_json
    df = df[cols_to_keep]

    # fix this strange column
    df['tagData'].fillna("{'tags': [{'id': 'nan', 'name': 'nan', 'hiddenFromPlanningAndTrends': 'nan'}]}", inplace=True)
    df['tagData'] = df['tagData'].apply(lambda x: str(x).replace("{'tags': [", ""))
    df['tagData'] = df['tagData'].apply(lambda x: str(x).replace("]}", ""))

    # normalize json columns
    for col in cols_to_json:
        print(f"column is: {col}")

        # make it a proper json
        df[col] = df[col].apply(lambda x: str(x).replace("True", '"True"').replace("False", '"False"'))
        df[col] = df[col].apply(lambda x: re.sub("'", '"', x))
        df[col] = df[col].apply(lambda x: json.loads(x))

        # create new columns from each key
        for key in df[col][0].keys():
            df[f"{col}_{key}"] = df[col].apply(lambda x: x[key])

    # correct the ids
    cols_to_split = ['id', 'category_id', 'category_parentId', 'tagData_id']
    for col in cols_to_split:
        df[col] = df[col].apply(lambda x: x.split('_')[1] if '_' in x else x)

    # drop columns
    cols_to_drop += ['type', 'tagData_hiddenFromPlanningAndTrends', 'accountRef_hiddenFromPlanningAndTrends']
    df.drop(columns=cols_to_drop, inplace=True)
    
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
    df['py_insert_ts'] = df.apply(lambda x: datetime.now(), axis=1)

    # columns to keep
    keep_cols = ['type', 'bankAccountType', 'id', 'name', 'currentBalance']

    # apply conditions
    cleaned_df = df[cond_1 & cond_2 & cond_3][keep_cols]

    return cleaned_df

# other functions remain the same
def get_and_clean_data():

    # Initialize Mint object
    mint = Mint(MINT_USER,
                MINT_PASS,
                mfa_method='soft-token',
                mfa_token=MINT_TOKEN,
                headless=True
                )

    # Get transactions and accounts dataframes
    transactions_df, accounts_df = get_transactions_and_accounts(mint)

    # Clean the data
    cleaned_transactions_df = clean_transactions(transactions_df)
    cleaned_accounts_df = clean_accounts(accounts_df)

    return cleaned_transactions_df, cleaned_accounts_df

# get them
cleaned_transactions_df, cleaned_accounts_df = get_and_clean_data()

# write to database
engine = create_engine(sql_addr)
cleaned_transactions_df.to_sql('transactions_history', engine, if_exists='append', index=False)
cleaned_accounts_df.to_sql('accounts_history', engine, if_exists='append', index=False)

print('done')
