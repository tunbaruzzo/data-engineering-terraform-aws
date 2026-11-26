
# ============================================================
# ARTEFACTO S3 - APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# Publica el paquete de la aplicacion Flink en el
# bucket S3 existente del entorno DEV.
#
# El ZIP se genera localmente y contiene:
# - main.py
# - lib/pyflink-dependencies.jar

resource "aws_s3_object" "flink_application" {

  # Reutilizamos el bucket del modulo Storage.
  bucket = module.storage.data_bucket_name

  # Ubicacion del artefacto dentro de S3.
  key = "artifacts/flink/application.zip"

  # Archivo local que Terraform subira al bucket.
  source = "${path.module}/../../flink/application.zip"

  # Detecta cambios en el contenido del ZIP.
  source_hash = filemd5(
    "${path.module}/../../flink/application.zip"
  )

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
