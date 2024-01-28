###############################################################################
# Name: ingest_data.py
# Description: This script retrieves the Melbourne Airbnb listing data via API
#              and loads into the bronze layer of the ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import requests
import shutil
import gzip
import io

import pandas as pd


# Download data file
url = 'http://data.insideairbnb.com/australia/vic/melbourne/2023-03-13/data/listings.csv.gz'
response = requests.get(url)

# Use BytesIO for in-memory file handling
gzip_file = io.BytesIO(response.content)

# Unzip file and load into a DataFrame
with gzip.open(gzip_file, 'rt') as f_in:  # 'rt' mode for text reading
    df = pd.read_csv(f_in)
print(df.head())


if __name__ == "__main__":
    print("hello")