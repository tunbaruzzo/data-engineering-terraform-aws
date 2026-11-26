
# ============================================================
# VARIABLES DEL MODULO FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# ============================================================
# ENTORNO
# ============================================================

variable "environment" {
  description = "Entorno donde se desplegara Apache Flink"
  type        = string
}

# ============================================================
# REGION AWS
# ============================================================

variable "aws_region" {
  description = "Region donde se encuentran los recursos AWS"
  type        = string
}

# ============================================================
# KINESIS DATA STREAM
# ============================================================

variable "kinesis_stream_arn" {
  description = "ARN del stream Kinesis existente"
  type        = string
}

# ============================================================
# ALMACENAMIENTO S3
# ============================================================

variable "data_bucket_arn" {
  description = "ARN del bucket S3 existente"
  type        = string
}

variable "data_bucket_name" {
  description = "Nombre del bucket S3 existente"
  type        = string
}

# ============================================================
# ARTEFACTO DE FLINK
# ============================================================

variable "flink_artifact_key" {
  description = "Ruta del ZIP de Flink dentro del bucket S3"
  type        = string
  default     = "artifacts/flink/application.zip"
}


# ============================================================
# VERSION DEL ARTEFACTO FLINK EN S3
# ============================================================

variable "flink_artifact_version" {
  description = "Version del objeto ZIP almacenado en S3"
  type        = string
}
