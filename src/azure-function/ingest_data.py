###############################################################################
# Name: ingest_data.py
# Description: This script fetches the Melbourne Airbnb listing data via API
#              and loads into the bronze layer of the data lakehouse.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import requests
import gzip
import yaml
import io
import sys
sys.path.append("./src")

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from utils import utils


def fetch_data(url):
    """This function fetches Airbnb data using
    the given URL, and returns it as a dataframe.

    Parameters
    ----------
    url: str
        Airbnb data API URL

    Returns
    -------
    df: pandas dataframe
        Fetched dataset
    """
    # Download data file
    response = requests.get(url)

    # Use BytesIO for in-memory file handling
    gzip_file = io.BytesIO(response.content)

    # Unzip file and load into a DataFrame
    with gzip.open(gzip_file, "rb") as f_in:
        df = pd.read_csv(f_in)

    return df


def load_storage_cred(yaml_path):
    """This function loads Azure storage account
    credentials from the given YAML file.

    Parameters
    ----------
    yaml_path: str
        Path to YAML file

    Returns
    -------
    account_name: str
        Storage account name
    account_key: str
        Storage account key
    """
    with open(yaml_path) as f:
        conf = yaml.safe_load(f)
        account_name = conf["account_name"]
        account_key = conf["account_key"]

    return (account_name, account_key)


def create_dir_client(
        account_name,
        account_key,
        container_name,
        directory_name
):
    """This function returns the Data Lake directory client.

    Parameters
    ----------
    account_name: str
        Azure storage account name
    account_key: str
        Azure storage account key
    container_name: str
        ADLS gen2 container name
    directory_name: str
        ADLS gen2 directory name

    Returns
    -------
    object
        Data Lake directory client
    """
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    file_system_client = service_client.get_file_system_client(file_system=container_name)

    return file_system_client.get_directory_client(directory=directory_name)


def upload_file(file, file_name, directory_client):
    """This function uploads the given bytes file into the
    ADLS directory.

    Parameters
    ----------
    file: bytes
        File to upload in bytes
    file_name: str
        File name
    directory_client: object
        Data Lake directory client

    Returns
    -------
    None
    """
    file_client = directory_client.create_file(file_name)
    file_client.upload_data(data=file, overwrite=True)
    file_client.flush_data(len(file))


def main():
    logger.info("Process has started.")

    # Fetch Airbnb listing datasets via API from 2023 March to 2023 June
    logger.info("Fetching Airbnb listing datasets...")
    df_list = []
    snapshot_dates = [
        "2023-03-13",
        "2023-04-09",
        "2023-05-13",
        "2023-06-06"
    ]
    for snapshot_date in snapshot_dates:
        logger.info(f"Fetching snapshot date {snapshot_date}...")
        url = f"http://data.insideairbnb.com/australia/vic/melbourne/{snapshot_date}" \
            "/data/listings.csv.gz"
        df = fetch_data(url)
        df_list.append(df)

    # Load Azure storage account credentials
    logger.info("Loading Azure credentials...")
    yaml_path = "./cred.yaml"
    account_name, account_key = load_storage_cred(yaml_path)

    # Create Data Lake directory client
    logger.info("Creating Data Lake directory client...")
    container_name = "airbnb-host-analytics"
    directory_name = "bronze"
    directory_client = create_dir_client(
        account_name,
        account_key,
        container_name,
        directory_name
    )

    # Upload dataset to ADLS as a parquet file
    logger.info("Uploading datasets to ADLS...")
    for i, dataset in enumerate(df_list):
        logger.info(f"Uploading snapshot date {snapshot_dates[i]}...")
        file_name = f"raw_dataset_{snapshot_dates[i]}.parquet"
        parquet_file = dataset.to_parquet()
        upload_file(parquet_file, file_name, directory_client)

    logger.info("Process has completed.")


if __name__ == "__main__":
    logger = utils.set_logger()
    main()
