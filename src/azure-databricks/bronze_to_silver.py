###############################################################################
# Name: bronze_to_silver.py
# Description: This script creates a dimensional data model from raw Airbnb
#              dataset and stores it as Delta Lake tables in the silver layer
#              of the ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################


# Define function to process dataset
def process_data(df):
    """This function processes the given dataset
    using Spark SQL and returns a dataframe.
    
    Parameters
    ----------
    df: dataframe
        Spark dataframe
        
    Returns
    -------
    dataframe
    """
    df.createOrReplaceTempView("dataset")
    df_airbnb_processed = spark.sql("""
    WITH BASE AS (
        SELECT
            TO_DATE(last_scraped, 'yyyy-MM-dd') AS SNAPSHOT_DATE,
            CAST(RIGHT(listing_url, POSITION('/' IN REVERSE(listing_url)) - 1) AS LONG) AS LISTING_ID,
            host_id AS HOST_ID,
            ROUND(
                DATEDIFF(
                    CURRENT_DATE,
                    TO_DATE(host_since, 'yyyy-MM-dd')
                ) / 365,
                0
            ) AS HOST_YEAR_OF_EXP,
            CAST(
                LENGTH(RTRIM(LTRIM(host_about)))
                - LENGTH(REPLACE(RTRIM(LTRIM(host_about)), ' ', ''))
                + 1 
                AS NUMERIC
            ) AS HOST_ABOUT_WORD_COUNT,
            host_is_superhost AS IS_HOST_SUPERHOST,
            host_has_profile_pic AS HAS_HOST_PROFILE_PHOTO,
            calculated_host_listings_count AS HOST_LISTING_COUNT,
            latitude AS LISTING_LATITUDE,
            longitude AS LISTING_LONGITUDE,
            CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC) AS LISTING_PRICE,
            PERCENTILE_APPROX(
                CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC),
                0.50
            ) OVER() AS LISTING_PRICE_2Q,
            PERCENTILE_APPROX(
                CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC),
                0.75
            ) OVER() * 1.5 AS LISTING_PRICE_UPPER,
            ROUND((30 - CAST(availability_30 AS NUMERIC)) / 30 * 100, 0) AS LISTING_OCCUPANCY_RATE,
            number_of_reviews AS LISTING_REVIEW_COUNT,
            neighbourhood_cleansed AS LISTING_MUNICIPALITY
        FROM dataset
        WHERE 1=1
            AND host_identity_verified = 't'
            AND availability_60 <> 0
            AND availability_90 <> 0
            AND availability_365 <> 0
            AND LOWER(neighbourhood_cleansed) IN (
                'banyule',
                'bayside',
                'boroondara',
                'brimbank',
                'cardinia',
                'casey',
                'darebin',
                'frankston',
                'glen eira',
                'greater dandenong',
                'hobsons bay',
                'hume',
                'kingston',
                'knox',
                'manningham',
                'maribyrnong',
                'maroondah',
                'melbourne',
                'melton',
                'monash',
                'moonee valley',
                'moreland',
                'mornington peninsula',
                'nillumbik',
                'port phillip',
                'stonnington',
                'whitehorse',
                'whittlesea',
                'wyndham',
                'yarra',
                'yarra ranges'
            )
            AND LOWER(room_type) = 'entire home/apt'
    )
    SELECT
        SNAPSHOT_DATE,
        LISTING_ID,
        HOST_ID,
        HOST_YEAR_OF_EXP,
        HOST_ABOUT_WORD_COUNT,
        IS_HOST_SUPERHOST,
        HAS_HOST_PROFILE_PHOTO,
        HOST_LISTING_COUNT,
        LISTING_LATITUDE,
        LISTING_LONGITUDE,
        LISTING_OCCUPANCY_RATE,
        LISTING_PRICE,
        LISTING_REVIEW_COUNT,
        LISTING_MUNICIPALITY
    FROM BASE
    WHERE 1=1
        AND LISTING_PRICE >= LISTING_PRICE_2Q
        AND LISTING_PRICE <= LISTING_PRICE_UPPER
    """)

    return df_airbnb_processed


def main():
    print("Process has started.")
    # Configure storage account credentials
    print("Configuring storage account credentials...")
    storage_account_name = dbutils.secrets.get(scope="key-vault-secret",key="storage-account-name")
    spark.conf.set(
        f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
        dbutils.secrets.get(scope="key-vault-secret",key="storage-account-key")
    )

    # Define file paths
    source_location = f"abfss://airbnb-host-analytics@{storage_account_name}.dfs.core.windows.net/bronze/raw_dataset.parquet"
    target_location = f"abfss://airbnb-host-analytics@{storage_account_name}.dfs.core.windows.net/silver"

    # Load raw Airbnb dataset
    print("Loading raw Airbnb dataset...")
    df_raw = spark.read.format("parquet").load(source_location)

    # Process raw dataset
    """
    The raw dataset is processed and filtered with the following conditions:
    - Host identity must be verified for valid hosts.
    - Room availability for next 60, 90 and 365 days must not be zero to ensure
      only available listings are used.
    - Municipality of the listing must be metropolitan municipalities to eliminate
      rural area effect.
    - Listing must be an entire home/apt to limit diversity of the listing type.
    - Price must be within the range between median and upper limit to filter out
      outliers and reduce price factor.
    
    *Metropolitan Melbourne municipalities: Banyule, Bayside, Boroondara, Brimbank,
    Cardinia, Casey, Darebin, Frankston, Glen Eira, Greater Dandenong, Hobsons Bay,
    Hume, Kingston, Knox, Manningham, Maribyrnong, Maroondah, Melbourne, Melton,
    Monash, Moonee Valley, Moreland, Mornington Peninsula, Nillumbik, Port Phillip,
    Stonnington, Whitehorse, Whittlesea, Wyndham, Yarra, Yarra Ranges
    """
    print("Processing raw dataset...")
    df_airbnb_processed = process_data(df_raw)

    # Create dimensional model
    ## Host dimension table

    ## Listing dimension table

    ## Listing occupancy fact table
    """"""

    # Store data model as delta lake tables in silver layer
    print("Saving Delta Lake tables in silver layer...")
    df_airbnb_processed.write.format("delta").mode("overwrite").save(target_location)
    print("Process has completed.")

if __name__ == "__main__":
    main()