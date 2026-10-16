# ============================================================
# IAM - ROL DE EJECUCION DE FIREHOSE
# ============================================================

# Define que servicio AWS puede asumir este rol.
data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Crea el rol que utilizara Firehose.
resource "aws_iam_role" "firehose" {
  name = "data-engineering-${var.environment}-firehose-role"

  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}



# ============================================================
# IAM - PERMISOS DE FIREHOSE
# ============================================================

data "aws_iam_policy_document" "firehose_permissions" {

  # Permite leer eventos del Kinesis Data Stream.
  statement {
    sid    = "ReadKinesisStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards"
    ]

    resources = [
      aws_kinesis_stream.ingestion.arn
    ]
  }

  # Permite entregar archivos en la zona Bronze de S3.
  statement {
    sid    = "WriteS3Bronze"
    effect = "Allow"

    actions = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
    ]

    resources = [
      "${var.data_bucket_arn}/${trim(var.data_prefix, "/")}/*"
    ]
  }

  # Permisos sobre el bucket para las operaciones de Firehose.
  statement {
    sid    = "AccessS3Bucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      var.data_bucket_arn
    ]
  }

  # Permite publicar logs en CloudWatch Logs.
  statement {
    sid    = "WriteCloudWatchLogs"
    effect = "Allow"

    actions = [
      "logs:PutLogEvents"
    ]

    resources = [
      "${aws_cloudwatch_log_group.firehose.arn}:log-stream:*"
    ]
  }

  # Permite utilizar la clave KMS administrada por AWS
  # para descifrar registros del stream.
  statement {
    sid    = "DecryptKinesisRecords"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey"
    ]

    resources = [
      data.aws_kms_key.kinesis.arn
    ]
  }
}

# Clave KMS administrada por AWS para Kinesis.
data "aws_kms_key" "kinesis" {
  key_id = "alias/aws/kinesis"
}

# Asocia la politica de permisos con el rol Firehose.
resource "aws_iam_role_policy" "firehose" {
  name   = "data-engineering-${var.environment}-firehose-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}