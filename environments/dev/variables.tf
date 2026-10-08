# ============================================================
# VARIABLES DEL ENTORNO DEV
# ============================================================

# Nombre del entorno de despliegue.
# Permite identificar los recursos pertenecientes a DEV.
variable "environment" {
  description = "Nombre del entorno de despliegue"
  type        = string
  default     = "dev"
}

# Región de AWS donde se desplegará la infraestructura.
variable "aws_region" {
  description = "Región de AWS donde se desplegará la infraestructura"
  type        = string
  default     = "us-east-1"
}

# Rango de direcciones IP utilizado por la VPC.
# Se define como variable para evitar hardcodear el CIDR
# dentro del módulo de red.
variable "vpc_cidr" {
  description = "CIDR principal de la VPC"
  type        = string
  default     = "10.0.0.0/16"
}