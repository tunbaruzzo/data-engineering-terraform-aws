
# ============================================================
# IAM - PERMISOS DE CLOUDWATCH PARA FLINK
# PRE-ENTREGA 04 / 05 - DATA ENGINEERING
# ============================================================

# Permisos del grupo IAM existente para administrar
# los recursos CloudWatch de la aplicacion Flink.
#
# Se conservan los permisos originales de administracion.
#
# Los permisos de lectura de eventos se definen en una
# politica administrada independiente para evitar superar
# el limite de tamano de las politicas inline del grupo.
#
# No modifica los permisos existentes de Firehose.


# ============================================================
# 1. POLITICA INLINE - ADMINISTRACION CLOUDWATCH
# ============================================================

resource "aws_iam_group_policy" "flink_cloudwatch_deployer" {

  name  = "DataEngineeringFlinkCloudWatchAccess"
  group = "data-engineering-lab"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ------------------------------------------------------
      # ADMINISTRAR ETIQUETAS DEL GRUPO DE LOGS
      # ------------------------------------------------------

      {
        Sid    = "ManageFlinkLogGroupTags"
        Effect = "Allow"

        Action = [
          "logs:TagResource",
          "logs:TagLogGroup",
          "logs:UntagResource",
          "logs:ListTagsForResource"
        ]

        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesis-analytics/data-engineering-${var.environment}-flink",
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesis-analytics/data-engineering-${var.environment}-flink:*"
        ]
      },

      # ------------------------------------------------------
      # ADMINISTRAR LOG STREAMS
      # ------------------------------------------------------

      {
        Sid    = "ManageFlinkLogStream"
        Effect = "Allow"

        Action = [
          "logs:CreateLogStream",
          "logs:DeleteLogStream",
          "logs:DescribeLogStreams"
        ]

        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesis-analytics/data-engineering-${var.environment}-flink:log-stream:*"
      }
    ]
  })
}


# ============================================================
# 2. POLITICA ADMINISTRADA - LECTURA DE LOGS
# ============================================================

# Permite consultar los eventos de CloudWatch de Flink
# para diagnosticar errores y validar el procesamiento.
#
# No permite modificar ni eliminar los eventos.

resource "aws_iam_policy" "flink_logs_readonly" {

  name        = "DataEngineeringFlinkLogsReadOnly"
  description = "Lectura de logs de Flink para diagnostico"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadFlinkLogEvents"
        Effect = "Allow"

        Action = [
          "logs:FilterLogEvents",
          "logs:GetLogEvents"
        ]

        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesis-analytics/data-engineering-${var.environment}-flink",
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kinesis-analytics/data-engineering-${var.environment}-flink:*"
        ]
      }
    ]
  })

  tags = {
    Project     = "data-engineering"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}


# ============================================================
# 3. ASOCIAR LA POLITICA ADMINISTRADA AL GRUPO IAM
# ============================================================

resource "aws_iam_group_policy_attachment" "flink_logs_readonly" {

  group      = "data-engineering-lab"
  policy_arn = aws_iam_policy.flink_logs_readonly.arn
}
