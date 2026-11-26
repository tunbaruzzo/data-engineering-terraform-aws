
# ============================================================
# AMAZON ATHENA - PERMISOS DE CONSULTA DEL LAKEHOUSE
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

# Politica IAM administrada para evitar superar el limite
# de tamano de las politicas inline del grupo.
#
# Permite consultar la tabla Iceberg desde Amazon Athena,
# utilizando el workgroup primary y el bucket S3 existente.

resource "aws_iam_policy" "athena_lakehouse_query" {

  name        = "DataEngineeringAthenaLakehouseQuery"
  description = "Permisos de consulta Athena para Lakehouse Pre05"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ======================================================
      # CONSULTAS EN ATHENA - WORKGROUP PRIMARY
      # ======================================================

      {
        Sid    = "AthenaPrimaryWorkgroup"
        Effect = "Allow"

        Action = [
          "athena:GetWorkGroup",
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetQueryRuntimeStatistics",
          "athena:ListQueryExecutions",
          "athena:BatchGetQueryExecution",
          "athena:StopQueryExecution"
        ]

        Resource = "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
      },

      # ======================================================
      # CONSULTA DEL CATALOGO DE DATOS
      # ======================================================

      {
        Sid    = "AthenaCatalogRead"
        Effect = "Allow"

        Action = [
          "athena:GetDataCatalog",
          "athena:GetDatabase",
          "athena:GetTableMetadata",
          "athena:ListDatabases",
          "athena:ListTableMetadata"
        ]

        Resource = "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:datacatalog/AwsDataCatalog"
      },

      # ======================================================
      # LISTADOS NECESARIOS PARA LA CONSOLA DE ATHENA
      # ======================================================

      {
        Sid    = "AthenaConsoleLists"
        Effect = "Allow"

        Action = [
          "athena:ListWorkGroups",
          "athena:ListDataCatalogs"
        ]

        Resource = "*"
      },

      # ======================================================
      # ACCESO AL BUCKET S3 DEL DATA LAKE
      # ======================================================

      {
        Sid    = "LakehouseBucketAccess"
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]

        Resource = "arn:aws:s3:::${module.storage.data_bucket_name}"
      },

      # ======================================================
      # LECTURA DE DATOS ICEBERG
      # ======================================================

      {
        Sid    = "ReadLakehouseIceberg"
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = "arn:aws:s3:::${module.storage.data_bucket_name}/lakehouse/transaction_metrics/*"
      },

      # ======================================================
      # RESULTADOS DE CONSULTAS ATHENA EN S3
      # ======================================================

      {
        Sid    = "AthenaQueryResults"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]

        Resource = "arn:aws:s3:::${module.storage.data_bucket_name}/athena-results/pre05/*"
      }

    ]
  })
}

# ============================================================
# ASOCIACION DE LA POLITICA ADMINISTRADA AL GRUPO IAM
# ============================================================

resource "aws_iam_group_policy_attachment" "athena_lakehouse_query" {

  group      = "data-engineering-lab"
  policy_arn = aws_iam_policy.athena_lakehouse_query.arn
}
