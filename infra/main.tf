# =============================================================================
# Resource Group
# =============================================================================
resource "azurerm_resource_group" "data_platform" {
  location = var.resource_group_location
  name     = "rg-${var.project_name}-${var.env}"
}

# =============================================================================
# 1. ADLS (Storage Account + ADLS Gen2)
# =============================================================================
resource "azurerm_storage_account" "adls" {
  name                     = "adls${var.project_name}${var.env}"
  resource_group_name      = azurerm_resource_group.data_platform.name
  location                 = var.resource_group_location
  account_tier             = "Standard"
  account_replication_type = var.env == "prod" ? "GRS" : "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "container" {
  for_each           = toset(["landing", "bronze", "silver", "gold", "quarantine"])
  name               = each.value
  storage_account_id = azurerm_storage_account.adls.id
}

# =============================================================================
# 2. Key Vault
# =============================================================================
resource "azurerm_key_vault" "kv" {
  name                        = "kv${var.project_name}${var.env}"
  location                    = var.resource_group_location
  tenant_id                   = var.tenant_id
  resource_group_name         = azurerm_resource_group.data_platform.name
  sku_name                    = "standard"
  rbac_authorization_enabled  = true
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
  principal_id          = azuread_service_principal.db_job.object_id
}

resource "azurerm_key_vault_secret" "nsw_transport_api_key" {
  name         = "nsw-transport-api-key"
  value        = var.nsw_transport_api_key
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_role_assignment.dev_kv_officer
  ]
}

# --- SP credentials, stored in Key Vault for Databricks to bootstrap from ---

resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "sp-client-id"
  value        = azuread_application.db_job.client_id
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_role_assignment.dev_kv_officer
  ]
}

resource "azurerm_key_vault_secret" "sp_tenant_id" {
  name         = "sp-tenant-id"
  value        = data.azurerm_client_config.current.tenant_id
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_role_assignment.dev_kv_officer
  ]
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "sp-client-secret"
  value        = azuread_service_principal_password.db_job.value
  key_vault_id = azurerm_key_vault.kv.id

  depends_on = [
    azurerm_role_assignment.dev_kv_officer
  ]
}

# =============================================================================
# 3. Databricks
# =============================================================================
resource "azurerm_databricks_workspace" "db" {
  name                 = "db${var.project_name}${var.env}"
  resource_group_name  = azurerm_resource_group.data_platform.name
  location             = var.resource_group_location
  sku                  = "trial"
}

# Create the service principal for the Databricks job
resource "azuread_application" "db_job" {
  display_name = "sp-databricks-job-${var.project_name}-${var.env}"
}

resource "azuread_service_principal" "db_job" {
  client_id = azuread_application.db_job.client_id
}

resource "azuread_service_principal_password" "db_job" {
  service_principal_id = azuread_service_principal.db_job.id
}

# Key-Vault-backed secret scope — jobs.yml references this as
# {{secrets/bootstrap-creds/<key>}}
resource "databricks_secret_scope" "bootstrap_creds" {
  name = "bootstrap-creds"

  keyvault_metadata {
    resource_id = azurerm_key_vault.kv.id
    dns_name    = azurerm_key_vault.kv.vault_uri
  }
}

# =============================================================================
# 4. Monitor
# =============================================================================
resource "azurerm_monitor_workspace" "monitor" {
  name                = "monitor${var.project_name}${var.env}"
  resource_group_name = azurerm_resource_group.data_platform.name
  location            = var.resource_group_location
}

resource "azurerm_role_assignment" "storage_blob_data_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "job_storage_blob_data_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id          = azuread_service_principal.db_job.object_id
}

# Register the AAD service principal inside the Databricks workspace
resource "databricks_service_principal" "db_job" {
  application_id = azuread_application.db_job.client_id
  display_name   = azuread_application.db_job.display_name
}

# Grant the job's service principal read access to this scope
resource "databricks_secret_acl" "job_bootstrap_read" {
  scope      = databricks_secret_scope.bootstrap_creds.name
  principal  = databricks_service_principal.db_job.application_id
  permission = "READ"
}

# Grant your own user read access too, for local/manual `bundle run` testing
resource "databricks_secret_acl" "dev_bootstrap_read" {
  scope      = databricks_secret_scope.bootstrap_creds.name
  principal  = "tranthelong1406@gmail.com"
  permission = "READ"
}

data "azuread_service_principal" "databricks_platform" {
  display_name = "AzureDatabricks"
}

resource "azurerm_role_assignment" "databricks_platform_kv_access" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id          = data.azuread_service_principal.databricks_platform.object_id
}