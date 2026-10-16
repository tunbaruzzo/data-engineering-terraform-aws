# ============================================================
# VARIABLES DEL MODULO KINESIS
# ============================================================

# Entorno donde se desplegara el stream.
# Ejemplo: dev, qa, prod.
variable "environment" {
  description = "Entorno de despliegue"
  type        = string
}

# Cantidad de shards del Kinesis Data Stream.
# Cada shard soporta hasta 1 MiB/s de escritura.
variable "shard_count" {
  description = "Cantidad de shards del stream"
  type        = number
  default     = 2

  validation {
    condition     = var.shard_count >= 2
    error_message = "El stream debe tener al menos 2 shards."
  }
}

# ARN del bucket S3 existente.
# Lo recibiremos desde el modulo storage.
variable "data_bucket_arn" {
  description = "ARN del bucket S3 del Data Lake"
  type        = string
}

# Prefijo donde Firehose almacenara los datos.
# En nuestro proyecto utilizamos bronze.
variable "data_prefix" {
  description = "Prefijo S3 para los datos crudos"
  type        = string
  default     = "bronze"
}