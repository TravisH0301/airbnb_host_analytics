# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.2"
    }
  }

  required_version = ">= 1.1.0"
}

provider "azurerm" {
  features {}
}

# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = "myTFResourceGroup"
  location = "australiasoutheast"
}

# Create a storage account for ADLS gen2
resource "azurerm_storage_account" "adls_gen2" {
  name                     = "airbnbstorage007"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
}

# Create a container in the ADLS gen2
resource "azurerm_storage_data_lake_gen2_filesystem" "container" {
  name               = "azure-airbnb-host-analytics"
  storage_account_id = azurerm_storage_account.adls_gen2.id
}

# Create folders in the container
## Bronze layer
resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.container.name
  storage_account_id = azurerm_storage_account.adls_gen2.id
  resource           = "directory"
}
## Silver layer
resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.container.name
  storage_account_id = azurerm_storage_account.adls_gen2.id
  resource           = "directory"
}
## Gold layer
resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.container.name
  storage_account_id = azurerm_storage_account.adls_gen2.id
  resource           = "directory"
}
## Gold layer Dev
resource "azurerm_storage_data_lake_gen2_path" "gold-dev" {
  path               = "gold-dev"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.container.name
  storage_account_id = azurerm_storage_account.adls_gen2.id
  resource           = "directory"
}