# ============================================================
# OUTPUTS DEL BOOTSTRAP
# ============================================================

# Devuelve el nombre del bucket S3 creado para almacenar
# el estado remoto de Terraform.
output "terraform_state_bucket" {
  description = "Bucket S3 utilizado para almacenar el Terraform State"
  value       = aws_s3_bucket.terraform_state.bucket
}

# Devuelve el nombre de la tabla DynamoDB utilizada
# para gestionar el locking del Terraform State.
output "terraform_lock_table" {
  description = "Tabla DynamoDB utilizada para el locking del Terraform State"
  value       = aws_dynamodb_table.terraform_locks.name
}