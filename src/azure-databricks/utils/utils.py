###############################################################################
# Name: utils.py
# Description: This script contains utility modules used in the data processing.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import os
os.system("pip install pyyaml")
import yaml
import logging


def get_query(query_name):
    """This function retrieves a query from a
    YAML configuration file.
    
    Parameters
    ----------
    query_name: str
        Name of query in the conf file

    Returns
    -------
    query: str
        Retrieved query
    """
    # Retrieve query from conf file
    with open("./conf/sql.yaml") as f:
        conf = yaml.safe_load(f)
        query = conf[query_name]

    return query


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
    storage_account_key = dbutils.secrets.get(
        scope="key-vault-secret",key="storage-account-key"
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
        storage_account_key
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


def process_data(spark, df, query):
    """This function processes the given dataset
    using Spark SQL and returns a dataframe.
    
    Parameters
    ----------
    spark: object
        Spark session
    df: dataframe
        Spark dataframe
    query: str
        SQL query to execute
        
    Returns
    -------
    Spark dataframe
    """
    df.createOrReplaceTempView("dataset")
    return spark.sql(query)


def set_logger(
    name=None,
    level=logging.INFO,
    streamhandler=True,
    filehandler=False,
    filename="",
):
    """This function returns a configured logger object.

    Parameters
    ----------
    name : str or None, optional
        Name of the logger
    level : int, optional
        Logging level (e.g., logging.INFO, logging.DEBUG)
    streamhandler : bool, optional
        Indication of stream handler addition
    filehandler : bool, optional
        Indication of filehandler addition
    filename : str, optional
        Log file path for filehandler

    Returns
    -------
    logging.Logger
        A configured logging.Logger object.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if streamhandler:
        formatter = logging.Formatter(
            "%(asctime)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
        )
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    if filehandler and isinstance(filename, (str, Path)):
        fh = logging.FileHandler(filename=filename, mode="a")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger