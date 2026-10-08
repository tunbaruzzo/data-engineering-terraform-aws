# ============================================================
# OUTPUTS DEL MÓDULO DE IDENTIDAD
# ============================================================

# Devuelve el ARN del rol de ejecución para servicios
# de procesamiento de datos.
#
# Este ARN podrá ser utilizado más adelante por servicios
# como Lambda o Managed Service for Apache Flink.
output "data_processing_execution_role_arn" {
  description = "ARN del rol de ejecución para servicios de procesamiento"
  value       = aws_iam_role.data_processing_execution.arn
}

# Devuelve el nombre del rol de ejecución.
#
# Puede ser útil para asociar políticas o referenciar
# el rol desde otros recursos Terraform.
output "data_processing_execution_role_name" {
  description = "Nombre del rol de ejecución para servicios de procesamiento"
  value       = aws_iam_role.data_processing_execution.name
}

# Devuelve el ARN del rol de auditoría de solo lectura.
#
# Este rol estará destinado al plano de control y
# tareas de auditoría.
output "audit_readonly_role_arn" {
  description = "ARN del rol de auditoría de solo lectura"
  value       = aws_iam_role.audit_readonly.arn
}

# Devuelve el nombre del rol de auditoría.
output "audit_readonly_role_name" {
  description = "Nombre del rol de auditoría de solo lectura"
  value       = aws_iam_role.audit_readonly.name
}