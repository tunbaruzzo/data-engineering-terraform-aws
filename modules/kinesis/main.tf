# ============================================================
# KINESIS DATA STREAM
# ============================================================

# Recibe eventos en tiempo real enviados por productores.
# Utilizamos capacidad provisionada para controlar los shards.

resource "aws_kinesis_stream" "ingestion" {

  # Nombre dinamico basado en el entorno.
  name = "data-engineering-${var.environment}-ingestion"

  # Capacidad de escritura y lectura del stream.
  shard_count = var.shard_count

  # Modo de capacidad exigido por el checkpoint.
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  # Cifrado de los datos almacenados en el stream.
  encryption_type = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  # Tiempo de retencion de los registros: 24 horas.
  retention_period = 24

  # Etiquetas para identificar y organizar recursos.
  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}