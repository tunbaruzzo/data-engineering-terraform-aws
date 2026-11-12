
# ============================================================
# IAM - PERMISOS PARA DESPLEGAR CLOUDWATCH DE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# Amplia los permisos del grupo IAM existente
# exclusivamente para administrar los logs de Flink.
#
# No modifica la politica anterior de Firehose.

resource "aws_iam_group_policy" "flink_cloudwatch_deployer" {

  name  = "DataEngineeringFlinkCloudWatchAccess"
  group = "data-engineering-lab"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
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
