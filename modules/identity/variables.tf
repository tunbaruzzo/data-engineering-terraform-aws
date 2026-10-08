# ============================================================
# VARIABLES DEL MÓDULO DE IDENTIDAD
# ============================================================

# Nombre lógico del entorno.
# Ejemplos: dev, staging, prod.
variable "environment" {
  description = "Nombre del entorno que utiliza el módulo de identidad"
  type        = string
}

# ARN del bucket S3 de datos.
#
# Este valor llega desde el módulo storage y permite
# construir políticas IAM sin hardcodear el nombre del bucket.
variable "data_bucket_arn" {
  description = "ARN del bucket S3 principal de datos"
  type        = string
}

# Prefijo dentro del bucket al que podrá acceder
# el rol de procesamiento.
#
# En nuestro entorno DEV utilizamos: bronze
variable "data_prefix" {
  description = "Prefijo S3 autorizado para procesamiento"
  type        = string
}