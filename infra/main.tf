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

data "azurerm_client_config" "current" {}

# Your local identity — write access, so Terraform can create secrets
resource "azurerm_role_assignment" "dev_kv_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id          = data.azurerm_client_config.current.object_id
}

# Your local identity — read access, for local script testing
resource "azurerm_role_assignment" "dev_kv_reader" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id          = data.azurerm_client_config.current.object_id
}

# Databricks job identity — read-only, never Officer
resource "azurerm_role_assignment" "job_kv_reader" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id          = var.databricks_job_identity_object_id   # confirm this value first — see step 0 note below
}

resource "azurerm_key_vault_secret" "nsw_transport_api_key" {
  name         = "nsw-transport-api-key"
  value        = var.nsw_transport_api_key
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_role_assignment.dev_kv_officer
  ]
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

data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "storage_blob_data_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}