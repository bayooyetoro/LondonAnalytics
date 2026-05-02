resource "fabric_workspace" "london" {
  display_name = var.workspace_name
  capacity_id = var.capacity_id
  description = "London Analytics workspace created with Terraform"
  identity = {
    type = "SystemAssigned"
  }
}

resource "fabric_workspace_role_assignment" "personal_admin" {
  workspace_id = fabric_workspace.london.id
  principal = {
    id   = var.personal_user_id
    type = "User"
  }
  role = "Admin"
}

resource "fabric_lakehouse" "london" {
  display_name = var.lakehouse_name
  workspace_id = fabric_workspace.london.id

  depends_on = [fabric_workspace_role_assignment.personal_admin]
}