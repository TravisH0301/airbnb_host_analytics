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
    """
    storage_account_name = dbutils.secrets.get(
        scope="key-vault-secret",
        key="storage-account-name"
    )
    spark.conf.set(
        f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
        dbutils.secrets.get(scope="key-vault-secret",key="storage-account-key")
    )