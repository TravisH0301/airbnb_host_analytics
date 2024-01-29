###############################################################################
# Name: utils.py
# Description: This script contains utility modules used in the data processing.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################


def set_azure_storage_config(spark, dbutils):
    """This function sets the Azure storage configuration
    in Spark using Azure key vault.

    Note that Spark related objects need to be passed from
    the script running directly on the Databricks cluster.
    This is because the external module is outside of the 
    runtime scope, hence having no access to the runtime 
    Spark configurations and features.

    Parameters
    ----------
    spark: object
        Spark session
    dbutils: object
        dbutil object

    Returns
    -------
    storage_account_name: str
        Azure storage account name
    """
    storage_account_name = dbutils.secrets.get(
        scope="key-vault-secret",
        key="storage-account-name"
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
        dbutils.secrets.get(scope="key-vault-secret",key="storage-account-key")
    )

    return storage_account_name


def load_data_to_df(spark, dbutils, container_name, file_path, file_type):
    """This function reads a dataset from the given
    ADLS source file location and returns as a 
    Spark dataframe.
    
    Parameters
    ----------
    spark: object
        Spark session
    dbutils: object
        Databricks util object
    container_name: str
        ADLS container name
    file_path: str
        Source file path in ADLS
    file_type: str
        Dataset storage format - e.g., parquet or delta
        
    Returns
    -------
    Spark dataframe
    """
    storage_account_name = set_azure_storage_config(spark, dbutils)
    source_location = f"abfss://{container_name}@{storage_account_name}" \
        f".dfs.core.windows.net/{file_path}"
    
    return spark.read.format(file_type).load(source_location)


def load_df_to_adls(
        spark,
        dbutils,
        df,
        container_name,
        file_path,
        file_type,
        save_mode
    ):
    """This function loads Spark dataframe
    into the ADLS gen2.
    
    Parameters
    ----------
    spark: object
        Spark session
    dbutils: object
        Databricks util object
    df: Spark dataframe
        Spark dataframe to load
    container_name: str
        ADLS container name
    file_path: str
        Source file path in ADLS
    file_type: str
        Dataset storage format - e.g., parquet or delta
    save_mode: str
        Spark dataframe save mode - e.g., append, ignore, overwrite

    Returns
    -------
    None
    """
    storage_account_name = set_azure_storage_config(spark, dbutils)
    target_location = f"abfss://{container_name}@{storage_account_name}" \
        f".dfs.core.windows.net/{file_path}"
    df.write.format(file_type).mode(save_mode).save(target_location)
