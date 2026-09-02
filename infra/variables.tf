variable "resource_group_location" {
  type        = string
  default     = "australiaeast"
  description = "Location of the resource group."
}

variable "project_name" {
  type        = string
  default     = "dataplatform"
}

variable "env"{
  type = string
  default = "staging"
}

variable "tenant_id" {
  type = string
  default = "e88d08b4-bc2f-4d11-9891-7ecf2fa73d99"
}

variable "nsw_transport_api_key" {
  type      = string
  sensitive = true
}