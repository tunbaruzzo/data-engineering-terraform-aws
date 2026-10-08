# ============================================================
# OUTPUTS DEL MÓDULO DE STORAGE
# ============================================================

# Devuelve el nombre del bucket S3 de datos.
#
# Este valor podrá ser utilizado por otros módulos,
# por ejemplo el módulo de IAM, para construir políticas
# de acceso sin hardcodear el nombre del bucket.
output "data_bucket_name" {
  description = "Nombre del bucket S3 principal de datos"
  value       = aws_s3_bucket.data_lake.bucket
}

# Devuelve el ARN del bucket S3.
#
# El ARN se utiliza frecuentemente en políticas IAM
# para identificar de forma única el recurso.
output "data_bucket_arn" {
  description = "ARN del bucket S3 principal de datos"
  value       = aws_s3_bucket.data_lake.arn
}

# Devuelve el prefijo autorizado dentro del bucket.
#
# Este valor se utilizará después para limitar
# los permisos del rol de procesamiento a una zona
# específica, por ejemplo: bronze/
output "data_prefix" {
  description = "Prefijo S3 autorizado para procesamiento de datos"
  value       = var.data_prefix
}