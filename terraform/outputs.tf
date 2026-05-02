output "workspace_id" {
  value       = fabric_workspace.london.id
  description = "Fabric workspace ID — paste into .env"
}

output "lakehouse_id" {
  value       = fabric_lakehouse.london.id
  description = "Fabric lakehouse ID — paste into .env"
}