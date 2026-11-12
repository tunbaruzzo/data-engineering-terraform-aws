
# ============================================================
# IAM - ADMINISTRACION DE APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# Permisos para administrar exclusivamente nuestra
# aplicacion Flink desde Terraform y AWS CLI.

locals {
  flink_app_arn = "arn:aws:kinesisanalytics:${var.aws_region}:${data.aws_caller_identity.current.account_id}:application/data-engineering-${var.environment}-flink"

  flink_execution_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.environment}-flink-execution-role"
}

resource "aws_iam_group_policy" "flink_management" {

  name  = "DataEngineeringFlinkManagementAccess"
  group = "data-engineering-lab"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "CreateFlinkApplication"
        Effect = "Allow"

        # CreateApplication requiere Resource = "*".
        Action = [
          "kinesisanalytics:CreateApplication"
        ]

        Resource = "*"

        Condition = {
          StringEquals = {
            "aws:RequestTag/Project"     = "data-engineering"
            "aws:RequestTag/Environment" = var.environment
          }
        }
      },

      {
        Sid    = "ManageProjectFlinkApplication"
        Effect = "Allow"

        Action = [
          "kinesisanalytics:DescribeApplication",
          "kinesisanalytics:UpdateApplication",
          "kinesisanalytics:StartApplication",
          "kinesisanalytics:StopApplication",
          "kinesisanalytics:DeleteApplication",
          "kinesisanalytics:CreateApplicationPresignedUrl",
          "kinesisanalytics:TagResource",
          "kinesisanalytics:UntagResource",
          "kinesisanalytics:ListTagsForResource"
        ]

        Resource = local.flink_app_arn
      },

      {
        Sid    = "ListFlinkApplications"
        Effect = "Allow"

        Action = [
          "kinesisanalytics:ListApplications"
        ]

        Resource = "*"
      },

      {
        Sid    = "PassFlinkExecutionRole"
        Effect = "Allow"

        Action = [
          "iam:PassRole"
        ]

        Resource = local.flink_execution_role_arn

        Condition = {
          StringEquals = {
            "iam:PassedToService" = "kinesisanalytics.amazonaws.com"
          }
        }
      }
    ]
  })
}
