# Airbnb Host Analytics
This project aims to study the effect of Airbnb host characteristics on the performance of property listings measured in occupancy rates in the next 30 days. To achieve this, a reliable and robust end-to-end data solution has been designed and implemented.

## Architecture
<img src="./images/data_architecture_diagram.JPG" width="800">

The architecture of the end-to-end data solution comprises of a mix of open source and cloud tools hosted on Azure cloud environment. The data originates from the API server undergoing ingestion, storage, processing and lastly serving for analytics. Below describes how each tool participates in this data landscape.

- <b>Azure Functions</b>: Azure function app fetches Airbnb listing data from the API server and loads raw datasets as Parquet files into the Bronze layer of data lakehouse hosted on the Azure Data Lake Storage Gen 2.
- <b>Azure Data Lake Storage (ADLS) & Delta Lake</b>: Azure Data Lake Storage and Delta Lake support the data lakehouse enabling affordable, scalable storage with ACID transactions.
- <b>Apache Spark & Databricks</b>: Apache Spark is the main data processing engine running on the Databricks cluster. It interacts with the datasets sitting in the data lakehouse to progressively develope datasets through Bronze, Silver, Gold layers of the medallion architecture.
- <b>Azure Synapse</b>: The Gold layer dataset is copied into Azure Synapse from the data lakehouse. Azure Synapse is a datawarehouse servicing analytics and providing connections to the business intelligence tools. Note that Databricks also provides analytics features with its SQL warehouse and Dashboards. However due to limited resources of the Azure free-tier account, Azure Synapse was implemented instead for the analytics workload.
- <b>Power BI</b>: Power BI serves to provide data visualisations using the metric layer dataset.
- <b>Azure Key Vault</b>: Azure Key Vault is used to store credentials of the Azure storage account, allowing data processes to access datasets in the data lakehouse securely.
- <b>Great Expectations</b>: Great Expectations is used as a data qaulity framework ensuring resultant data assets to be validated and reliable in the Gold layer of the data lakehouse.
- <b>Apache Airflow & Azure Data Factory</b>: Apache Airflow orchestrates the data flow by triggering Azure function app and Spark jobs on Databricks. Airflow is hosted on Azure Data Factory as a managed service.
- <b>Terraform</b>: Terraform is used to
- <b>GitHub Actions</b>:

## Data Pipeline
<img src="./images/data_flow.JPG" width="800">
credentials Key vault
email alerts

## Data Modelling
SCD type 2
Monthly snapshot fact table
metric layer with the latest data

## Data Quality
write, audit, publish for gold layer data assets

## DevOps
### CI

### CD

### IaC

## Insights
<img src="./images/dashboard.JPG" width="800">
The dashboard showcases the influence of host characteristics on Airbnb listing occupancy rates. Key insights include:

- Super Host Status: Super Hosts tend to have higher occupancy rates, indicating that Airbnb's recognition of excellent service positively impacts bookings.

- Profile Photo: Hosts with profile photos see better occupancy rates, suggesting that photos may build trust with potential guests.

- Host About Description: Detailed and longer descriptions correlate with higher occupancy rates, implying that guests value thorough information about their hosts.

- Years of Experience: More experienced hosts enjoy higher occupancy rates, likely due to better hosting practices and established trust.

- Review Count: Listings with a higher count of reviews generally have greater occupancy rates, underscoring the importance of guest feedback.

- Winning Attributes & Rest: Hosts with "winning" attributes (those linked to higher occupancy rates) significantly outperform those without, highlighting the benefits of cultivating positive hosting traits.

Overall, the data suggests that personal touches and credibility markers like Super Host status, a clear profile photo, a rich host description, experience, and a high review count are key to enhancing an Airbnb listing's performance.

## Considerations
- <b>Data Lifecycle</b>:

## Data Source
This study is conducted over the Airbnb listing datasets provided by [Inside Airbnb](http://insideairbnb.com/).