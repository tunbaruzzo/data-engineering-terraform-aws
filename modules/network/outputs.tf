# ============================================================
# OUTPUTS DEL MÓDULO DE RED
# ============================================================

# Devuelve el ID de la VPC principal creada por este módulo.
#
# Este valor podrá ser utilizado por otros módulos o recursos
# que necesiten conocer en qué VPC deben desplegarse.
output "vpc_id" {
  description = "ID de la VPC principal"
  value       = aws_vpc.main.id
}

# Devuelve los IDs de las dos subredes privadas.
#
# Se agrupan en una lista porque normalmente los servicios
# que necesitan alta disponibilidad trabajan con más de una
# subred en distintas zonas de disponibilidad.
output "private_subnet_ids" {
  description = "IDs de las subredes privadas"
  value = [
    aws_subnet.private_a.id,
    aws_subnet.private_b.id
  ]
}

# Devuelve el ID de la tabla de rutas privada.
#
# Puede ser útil para futuras integraciones de red o para
# agregar nuevos endpoints y rutas.
output "private_route_table_id" {
  description = "ID de la tabla de rutas privada"
  value       = aws_route_table.private.id
}

# Devuelve el ID del Gateway Endpoint de S3.
#
# Este output permite identificar el endpoint privado
# utilizado por las subredes para acceder a Amazon S3.
output "s3_vpc_endpoint_id" {
  description = "ID del S3 Gateway Endpoint"
  value       = aws_vpc_endpoint.s3.id
}



