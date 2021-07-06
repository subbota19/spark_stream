terraform {
  required_providers {
    azurerm =  "~> 2.33"
    random = "~> 2.2"
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.3.4"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
  
}

data "azurerm_client_config" "current" {
}

data "external" "me" {
  program = ["az", "account", "show", "--query", "user"]
}

locals {
  prefix = "databricksdemo${random_string.naming.result}"
  tags = {
    Environment = "Demo"
    Owner       = lookup(data.external.me.result, "name")
  }
}

resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.region
  tags     = local.tags

}

resource "azurerm_databricks_workspace" "this" {
  name                        = "${var.prefix}-workspace"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "trial"
  managed_resource_group_name = "${var.prefix}-workspace-rg"
  tags                        = local.tags
  
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_cluster" "this" {
  cluster_name            = "Single Spark Node"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = 20

  spark_conf = {
    # Single-node
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : var.core_info
  }
  
  spark_env_vars = {
	"client_secret" : var.client_secret
	"provider_type" : var.provider_type
	"client_endpoint" : var.client_endpoint
	"account_key" : var.account_key
	"client_id" : var.client_id
	"auth_type" : var.auth_type
	"account_load_name" : var.account_load_name
	"account_upload_name" : var.account_upload_name
	"hotel_weather_path_edit" : var.hotel_weather_path_edit
	"hotel_weather_path" : var.hotel_weather_path
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}


output "databricks_host" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}/"
}

