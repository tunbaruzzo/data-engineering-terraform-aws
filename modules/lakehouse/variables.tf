
# ============================================================
# VARIABLES - MODULO LAKEHOUSE
# ============================================================

variable "environment" {
  description = "Entorno de despliegue del Lakehouse"
  type        = string
}


# ============================================================
# ALMACENAMIENTO S3 DEL LAKEHOUSE
# ============================================================

variable "data_bucket_name" {
  description = "Nombre del bucket S3 donde se almacenaran las tablas Iceberg"
  type        = string
}
