output "resource_group_name" {
  value = azurerm_resource_group.data_platform.name
}

output "db_job_client_id" {
  value = azuread_application.db_job.client_id
}

output "db_job_tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}

output "db_job_client_secret" {
  value     = azuread_service_principal_password.db_job.value
  sensitive = true
}