# ============================================================
# OUTPUTS DEL ENTORNO DEV
# ============================================================

# Este archivo expone hacia afuera los valores importantes
# generados por los módulos que componen el entorno DEV.


# ============================================================
# OUTPUTS DE RED
# ============================================================

# ID de la VPC creada por el módulo network.
output "vpc_id" {
  description = "ID de la VPC del entorno DEV"
  value       = module.network.vpc_id
}

# IDs de las subredes privadas creadas en distintas
# zonas de disponibilidad.
output "private_subnet_ids" {
  description = "IDs de las subredes privadas del entorno DEV"
  value       = module.network.private_subnet_ids
}

# ID de la tabla de rutas utilizada por las subredes privadas.
output "private_route_table_id" {
  description = "ID de la tabla de rutas privada del entorno DEV"
  value       = module.network.private_route_table_id
}

# ID del Gateway Endpoint utilizado para acceder
# privadamente a Amazon S3.
output "s3_vpc_endpoint_id" {
  description = "ID del S3 Gateway Endpoint del entorno DEV"
  value       = module.network.s3_vpc_endpoint_id
}


# ============================================================
# OUTPUTS DE IDENTIDAD
# ============================================================

# ARN del rol que utilizarán los servicios de procesamiento
# de datos como Lambda o Managed Service for Apache Flink.
output "data_processing_execution_role_arn" {
  description = "ARN del rol de ejecución para procesamiento de datos"
  value       = module.identity.data_processing_execution_role_arn
}

# ARN del rol destinado a tareas de auditoría.
output "audit_readonly_role_arn" {
  description = "ARN del rol de auditoría de solo lectura"
  value       = module.identity.audit_readonly_role_arn
}