from mintapi import Mint
import os
from dotenv import load_dotenv

# get env variables
load_dotenv()
MINT_USER = os.getenv('MINT_USER')
MINT_PASS = os.getenv('MINT_PASS')
MINT_TOKEN = os.getenv('MINT_TOKEN')

# Initialize Mint object
def get_mint_data():
    mint = Mint(MINT_USER,
                MINT_PASS,
                mfa_method='soft-token',
                mfa_token=MINT_TOKEN,
                headless=True
                )

    return mint
