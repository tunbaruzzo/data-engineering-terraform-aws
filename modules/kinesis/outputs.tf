# ============================================================
# OUTPUTS DEL MODULO KINESIS
# ============================================================

# Nombre del stream para productores y pruebas.
output "stream_name" {
  description = "Nombre del Kinesis Data Stream"
  value       = aws_kinesis_stream.ingestion.name
}

# ARN del stream para permisos IAM y Firehose.
output "stream_arn" {
  description = "ARN del Kinesis Data Stream"
  value       = aws_kinesis_stream.ingestion.arn
}