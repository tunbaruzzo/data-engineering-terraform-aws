# ============================================================
# CONFIGURACIÓN DE TERRAFORM PARA EL BOOTSTRAP DEL BACKEND
# ============================================================

terraform {

  # Provider oficial de AWS.
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Versión mínima de Terraform.
  required_version = ">= 1.16.0"
}

# ============================================================
# PROVIDER AWS
# ============================================================

# Región donde vamos a crear los recursos que almacenarán
# el estado remoto de Terraform.
provider "aws" {
  region = "us-east-1"
}