# ============================================================
# VARIABLES DEL MÓDULO DE STORAGE
# ============================================================

# Nombre lógico del entorno que está utilizando este módulo.
# Ejemplos: dev, staging, prod.
# Nos servirá para nombrar y etiquetar el bucket S3.
variable "environment" {
  description = "Nombre del entorno que utiliza el módulo de storage"
  type        = string
}

# ID de la cuenta AWS actual.
#
# Lo vamos a utilizar para construir un nombre de bucket
# globalmente único, ya que los nombres de buckets S3
# deben ser únicos en todo AWS.
variable "aws_account_id" {
  description = "ID de la cuenta AWS utilizada para generar un nombre único de bucket"
  type        = string
}

# Prefijo lógico dentro del bucket al que tendrá acceso
# el rol de procesamiento.
#
# Para esta preentrega usaremos una zona tipo Bronze,
# donde más adelante podrían almacenarse datos crudos.
variable "data_prefix" {
  description = "Prefijo S3 autorizado para los servicios de procesamiento"
  type        = string
  default     = "bronze"
}