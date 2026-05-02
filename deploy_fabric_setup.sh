#!/bin/bash

set -e

echo "========================================"
echo "Fabric Terraform Demo - Deploy Script"
echo "========================================"
echo ""

# Check if Terraform is installed
if ! command -v terraform &> /dev/null; then
    echo "Error: Terraform is not installed"
    exit 1
fi

echo "✓ Prerequisites checked"
echo ""

# Initialize Terraform
cd terraform

echo "Step 1: Initializing Terraform..."
terraform init

echo ""
echo "Step 2: Planning deployment..."
terraform plan -out=tfplan

echo "================================================================================="
echo "Review the planned changes above. Make sure they look correct before applying."
echo "================================================================================="
echo ""

echo ""
echo "Step 3: Applying configuration..."
read -p "Do you want to apply this configuration? (yes/no): " confirm
if [ "$confirm" = "yes" ]; then
    terraform apply tfplan
    echo "================================================================================================="
    echo "✓ Deployment completed!" Check Microsoft Fabric portal to see your new workspace and lakehouse.
    echo "=================================================================================================="
    echo "Outputs:"
    terraform output
else
    echo "Deployment cancelled"
    rm -f tfplan
fi