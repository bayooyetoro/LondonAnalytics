variable "tenant_id" {
  type      = string
  sensitive = true
}

variable "capacity_id" {
  type      = string
  sensitive = true
}

variable "personal_user_id" {
  type      = string
  sensitive = true
}

variable "client_id" {
  type      = string
  sensitive = true
}

variable "client_secret" {
  type      = string
  sensitive = true
}

variable "workspace_name" {
  type    = string
  default = "LondonAnalytics"
}

variable "lakehouse_name" {
  type    = string
  default = "london_lakehouse"
}