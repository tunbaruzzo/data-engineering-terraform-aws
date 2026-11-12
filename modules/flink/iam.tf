
# ============================================================
# IAM - AMAZON MANAGED SERVICE FOR APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# Informacion de la cuenta AWS actual.
data "aws_caller_identity" "current" {}

# ============================================================
# NOMBRES DE RECURSOS
# ============================================================

locals {
  flink_application_name = "data-engineering-${var.environment}-flink"

  flink_log_group_name = "/aws/kinesis-analytics/${local.flink_application_name}"

  flink_log_stream_name = "kinesis-analytics-log-stream"

  flink_log_group_arn = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${local.flink_log_group_name}"

  flink_log_stream_arn = "${local.flink_log_group_arn}:log-stream:${local.flink_log_stream_name}"
}

# ============================================================
# ROL DE EJECUCION DE FLINK
# ============================================================

resource "aws_iam_role" "flink_execution" {

  name = "${var.environment}-flink-execution-role"

  # Solo Managed Service for Apache Flink puede asumirlo.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"

        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-flink-execution-role"
    Environment = var.environment
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# POLITICA DE PERMISOS
# ============================================================

resource "aws_iam_role_policy" "flink_permissions" {

  name = "${var.environment}-flink-permissions"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ------------------------------------------------------
      # LECTURA DEL KINESIS DATA STREAM
      # ------------------------------------------------------

      {
        Sid    = "ReadKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListShards",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords"
        ]

        Resource = var.kinesis_stream_arn
      },


      # ------------------------------------------------------
      # LECTURA DEL ARTEFACTO EN S3
      # ------------------------------------------------------

      {
        Sid    = "ReadFlinkArtifact"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = "${var.data_bucket_arn}/${var.flink_artifact_key}"
      },

      # ------------------------------------------------------
      # CONSULTAR GRUPOS DE LOGS
      # ------------------------------------------------------

      {
        Sid    = "DescribeLogGroups"
        Effect = "Allow"

        Action = [
          "logs:DescribeLogGroups"
        ]

        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:*"
      },

      # ------------------------------------------------------
      # CONSULTAR STREAMS DE LOGS
      # ------------------------------------------------------

      {
        Sid    = "DescribeLogStreams"
        Effect = "Allow"

        Action = [
          "logs:DescribeLogStreams"
        ]

        Resource = "${local.flink_log_group_arn}:log-stream:*"
      },

      # ------------------------------------------------------
      # ESCRIBIR LOGS DE FLINK
      # ------------------------------------------------------

      {
        Sid    = "WriteFlinkLogs"
        Effect = "Allow"

        Action = [
          "logs:PutLogEvents"
        ]

        Resource = local.flink_log_stream_arn
      }
    ]
  })
}
