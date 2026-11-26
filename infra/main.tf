# ============================================================
# INFRAESTRUCTURA LAKEHOUSE LOCAL
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0, < 7.0"
    }
  }
}

# ============================================================
# PROVIDER AWS - LOCALSTACK
# ============================================================

# Credenciales ficticias y endpoints exclusivamente locales.

provider "aws" {
  region     = "us-east-1"
  access_key = "test"
  secret_key = "test"

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  s3_use_path_style = true

  endpoints {
    s3      = "http://localhost:4566"
    kinesis = "http://localhost:4566"
    iam     = "http://localhost:4566"
    sts     = "http://localhost:4566"
    glue    = "http://localhost:4566"
  }
}

data "aws_caller_identity" "current" {}

# ============================================================
# BUCKET S3 DEL LAKEHOUSE
# ============================================================

resource "aws_s3_bucket" "lakehouse" {
  bucket = "lakehouse"

  tags = {
    Environment = "local"
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# VERSIONADO DEL BUCKET
# ============================================================

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ============================================================
# AWS GLUE DATA CATALOG
# ============================================================

# Glue no se despliega en el laboratorio local.
# El metastore local utiliza JDBC Catalog sobre PostgreSQL.
# La declaracion se conserva para su validacion con Terraform.
# La implementacion real de AWS esta en modules/lakehouse.

resource "aws_glue_catalog_database" "lakehouse_db" {
  count = 0

  name        = "lakehouse_db"
  description = "Catalogo de tablas Apache Iceberg"
}

# ============================================================
# KINESIS DATA STREAM
# ============================================================

resource "aws_kinesis_stream" "transactions" {
  name        = "data-engineering-local-stream"
  shard_count = 2

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = "local"
    Project     = "data-engineering"
  }
}

# ============================================================
# ROL DE EJECUCION DE FLINK
# ============================================================

resource "aws_iam_role" "flink_execution" {
  name = "local-flink-execution-role"

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
}

# ============================================================
# PERMISOS IAM DE FLINK
# ============================================================

resource "aws_iam_role_policy" "flink_permissions" {
  name = "local-flink-lakehouse-permissions"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # Lectura de eventos desde Kinesis.
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

        Resource = aws_kinesis_stream.transactions.arn
      },

      # Permisos sobre el catalogo Glue para AWS.
      {
        Sid    = "ManageLakehouseCatalog"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable"
        ]

        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/lakehouse_db",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/lakehouse_db/*"
        ]
      },

      # Listado de objetos del bucket.
      {
        Sid    = "ListLakehouseObjects"
        Effect = "Allow"

        Action = [
          "s3:ListBucket"
        ]

        Resource = aws_s3_bucket.lakehouse.arn
      },

      # Lectura y escritura de datos y metadatos Iceberg.
      {
        Sid    = "ManageLakehouseObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]

        Resource = "${aws_s3_bucket.lakehouse.arn}/warehouse/*"
      }
    ]
  })
}

# ============================================================
# OUTPUTS
# ============================================================

output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "iceberg_warehouse" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse"
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.transactions.name
}

output "glue_database_name" {
  value = "lakehouse_db"
}