resource "azurerm_resource_group" "data_platform" {
  location = var.resource_group_location
  name     = "rg-${var.project_name}-${var.env}"
}

// 1. adls (storage account + adls gen 2)

resource "azurerm_storage_account" "adls" {
    name = "adls${var.project_name}${var.env}"
    resource_group_name = azurerm_resource_group.data_platform.name
    location = var.resource_group_location
    account_tier = "Standard"
    account_replication_type = var.env == "prod" ? "GRS" : "LRS" 
    is_hns_enabled = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "container" {
  for_each           = toset(["landing", "bronze", "silver", "gold", "quarantine"])
  name               = each.value
  storage_account_id = azurerm_storage_account.adls.id
}

// 2.key_vault
resource "azurerm_key_vault" "kv" {
    name = "kv${var.project_name}${var.env}"
    location = var.resource_group_location
    tenant_id = var.tenant_id
    resource_group_name = azurerm_resource_group.data_platform.name
    sku_name = "standard"
    rbac_authorization_enabled = true
}

// 3. databricks
resource "azurerm_databricks_workspace" "db" {
    name = "db${var.project_name}${var.env}"
    resource_group_name = azurerm_resource_group.data_platform.name
    location = var.resource_group_location
    sku = "trial"
}

// 4. monitor
resource "azurerm_monitor_workspace" "monitor" {
  name                = "monitor${var.project_name}${var.env}"
  resource_group_name = azurerm_resource_group.data_platform.name
  location            = var.resource_group_location
}