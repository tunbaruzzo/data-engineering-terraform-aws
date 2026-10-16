# ============================================================
# AMAZON DATA FIREHOSE - ENTREGA A S3 BRONZE
# ============================================================

resource "aws_kinesis_firehose_delivery_stream" "ingestion" {

  name        = "data-engineering-${var.environment}-firehose"
  destination = "extended_s3"

  # Firehose consume los registros del Kinesis Data Stream.
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.ingestion.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  # Destino de almacenamiento: S3 Bronze.
  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.data_bucket_arn

    # Organizacion por fecha de entrega.
    prefix = "${trim(var.data_prefix, "/")}/ingesta/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    # Ruta para los registros que Firehose no logra entregar.
    error_output_prefix = "${trim(var.data_prefix, "/")}/errores/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = 5
    buffering_interval = 60

    compression_format = "UNCOMPRESSED"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }

  # Esperar a que la politica IAM este asociada.
  depends_on = [
    aws_iam_role_policy.firehose
  ]

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}