# ============================================================
# CONFIGURACIÓN GENERAL DE TERRAFORM Y AWS
# ============================================================

terraform {

  # Define los providers que necesita este proyecto.
  required_providers {

    # Provider oficial de AWS mantenido por HashiCorp.
    aws = {
      source = "hashicorp/aws"

      # Permite usar versiones compatibles dentro de la rama 6.x.
      version = "~> 6.0"
    }
  }

  # Versión mínima de Terraform requerida para ejecutar el proyecto.
  required_version = ">= 1.16.0"
}

# ============================================================
# PROVIDER DE AWS
# ============================================================

# Configura el provider de AWS que Terraform utilizará
# para crear y administrar los recursos del proyecto.
provider "aws" {

  # La región se obtiene desde variables.tf
  # en lugar de escribirla de forma fija en este archivo.
  region = var.aws_region
}