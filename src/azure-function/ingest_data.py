###############################################################################
# Name: ingest_data.py
# Description: This script retrieves the Melbourne Airbnb listing data via API
#              and loads into the bronze layer of the ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import requests

import pandas as pd
import shutil
import gzip


# Download data file
url = 'http://data.insideairbnb.com/australia/vic/melbourne/2023-03-13/data/listings.csv.gz'
response = requests.get(url, stream=True)

# Unzip file
with open('listings.csv.gz', 'wb') as f:
    response.raw.decode_content = True
    shutil.copyfileobj(response.raw, f)

with gzip.open('listings.csv.gz', 'rb') as f_in:
    with open('listings.csv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# Load unzipped CSV dataset into dataframe
df = pd.read_csv('listings.csv')

print(df.head())