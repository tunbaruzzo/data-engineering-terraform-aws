# ============================================================
# CLOUDWATCH ALARMS - KINESIS DATA STREAMS
# ============================================================

# Detecta cuando se supera la capacidad de escritura.
resource "aws_cloudwatch_metric_alarm" "write_throughput_exceeded" {
  alarm_name          = "data-engineering-${var.environment}-kinesis-write-exceeded"
  alarm_description   = "Detecta intentos de escritura que superan la capacidad provisionada de Kinesis."

  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  statistic           = "Sum"

  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"

  dimensions = {
    StreamName = aws_kinesis_stream.ingestion.name
  }

  treat_missing_data = "notBreaching"

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Detecta cuando se supera la capacidad de lectura.
resource "aws_cloudwatch_metric_alarm" "read_throughput_exceeded" {
  alarm_name          = "data-engineering-${var.environment}-kinesis-read-exceeded"
  alarm_description   = "Detecta intentos de lectura que superan la capacidad provisionada de Kinesis."

  namespace           = "AWS/Kinesis"
  metric_name         = "ReadProvisionedThroughputExceeded"
  statistic           = "Sum"

  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"

  dimensions = {
    StreamName = aws_kinesis_stream.ingestion.name
  }

  treat_missing_data = "notBreaching"

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}