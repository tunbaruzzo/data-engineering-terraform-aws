# ============================================================
# ARTEFACTO S3 - APACHE FLINK
# PRE-ENTREGA 05 - EXPERIMENTO ICEBERG / HADOOP
# ============================================================

# Publica el paquete de Flink en el bucket S3 de DEV.
#
# ZIP experimental validado:
# - main.py
# - lib/pyflink-dependencies.jar
#
# La publicacion se realizara mediante Terraform.

resource "aws_s3_object" "flink_application" {

  # Reutilizar el bucket del modulo Storage.
  bucket = module.storage.data_bucket_name

  # Mantener la ubicacion del artefacto en S3.
  key = "artifacts/flink/application.zip"

  # Paquete definitivo de Flink con integracion Apache Iceberg.
  source = "${path.module}/../../flink/application-pre05.zip"

  # Detectar cambios en el paquete para actualizar el artefacto.
  source_hash = filemd5("${path.module}/../../flink/application-pre05.zip")

  # Tipo de archivo.
  content_type = "application/zip"

  # Cifrado SSE-S3.
  server_side_encryption = "AES256"

  tags = {
    Name        = "flink-application"
    Environment = var.environment
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }
}
