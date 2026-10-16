# ============================================================
# CLOUDWATCH LOGS - FIREHOSE
# ============================================================

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/data-engineering-${var.environment}"
  retention_in_days = 7

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}