import os
import tempfile
import gzip
import shutil
import requests
import pandas as pd
from google.cloud import storage
import functions_framework

@functions_framework.http
def load_data_to_gcs(request):
    # Download data file
    url = 'http://data.insideairbnb.com/australia/vic/melbourne/2023-03-13/data/listings.csv.gz'
    response = requests.get(url, stream=True)

    # Create a temporary directory to store the downloaded file
    with tempfile.TemporaryDirectory() as tmpdir:
        # Unzip file
        file_path_gz = os.path.join(tmpdir, 'listings.csv.gz')
        with open(file_path_gz, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)

        file_path_csv = os.path.join(tmpdir, 'listings.csv')
        with gzip.open(file_path_gz, 'rb') as f_in:
            with open(file_path_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Load unzipped CSV dataset into dataframe
        df = pd.read_csv(file_path_csv)
        # Replace new lines with spaces
        """This is to prevent the loading error when reading CSV files in Big Query."""
        change_cols = [
            "name",
            "description",
            "neighborhood_overview",
            "host_about"
        ]
        for col in change_cols:
            df[col]= df[col].apply(lambda x: x.replace("\r"," ").replace("\n"," "))

        # Upload data to GCS bucket
        storage_client = storage.Client()
        bucket_name = 'airbnb_ingress'
        bucket = storage_client.get_bucket(bucket_name)
        blob_name = 'listings.csv'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

        return f'Successfully uploaded {blob_name} to {bucket_name} bucket.'