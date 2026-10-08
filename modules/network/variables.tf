# ============================================================
# VARIABLES DEL MÓDULO NETWORK
# ============================================================

# Entorno al que pertenecen los recursos de red.
variable "environment" {
  description = "Nombre del entorno que utiliza el módulo de red"
  type        = string
}

# Región AWS utilizada para determinar las Availability Zones.
variable "aws_region" {
  description = "Región de AWS utilizada por el módulo de red"
  type        = string
}

# CIDR principal que utilizará la VPC.
# El valor se recibe desde el entorno que consume este módulo.
variable "vpc_cidr" {
  description = "CIDR principal de la VPC"
  type        = string
}