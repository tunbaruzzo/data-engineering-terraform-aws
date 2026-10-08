# ============================================================
# MÓDULO DE IDENTIDAD - IAM
# ============================================================

# Este módulo administra las identidades y permisos
# utilizados por los servicios del pipeline.
#
# Crea:
# - Rol de ejecución para servicios de procesamiento
# - Política S3 de mínimo privilegio
# - Rol de auditoría de solo lectura


# ============================================================
# INFORMACIÓN DE LA CUENTA AWS ACTUAL
# ============================================================

# Consulta información de la cuenta AWS autenticada.
# No crea ningún recurso.
data "aws_caller_identity" "current" {}


# ============================================================
# ROL DE EJECUCIÓN PARA SERVICIOS DE DATOS
# ============================================================

# Este rol podrá ser asumido por servicios como
# AWS Lambda o Managed Service for Apache Flink.
resource "aws_iam_role" "data_processing_execution" {

  name = "${var.environment}-data-processing-execution-role"

  # Trust Policy:
  # define QUÉ servicios pueden asumir este rol.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"

        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "kinesisanalytics.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-data-processing-execution-role"
    Environment = var.environment
    Project     = "data-engineering"
  }
}


# ============================================================
# POLÍTICA S3 DE MÍNIMO PRIVILEGIO
# ============================================================

# Esta política NO permite acceso general a S3.
#
# El rol únicamente podrá:
#
# - listar objetos del prefijo autorizado
# - leer objetos del prefijo autorizado
# - escribir objetos en el prefijo autorizado
#
# En nuestro caso:
#
# bucket/
# └── bronze/
#       └── acceso permitido
#
resource "aws_iam_policy" "data_processing_s3" {

  name        = "${var.environment}-data-processing-s3-policy"
  description = "Acceso mínimo al prefijo autorizado del bucket de datos"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ------------------------------------------------------
      # LISTAR CONTENIDO DEL PREFIJO
      # ------------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "s3:ListBucket"
        ]

        # ListBucket trabaja sobre el ARN del bucket.
        Resource = var.data_bucket_arn

        # Restringe el listado al prefijo autorizado.
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.data_prefix}",
              "${var.data_prefix}/*"
            ]
          }
        }
      },

      # ------------------------------------------------------
      # LEER Y ESCRIBIR OBJETOS
      # ------------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]

        # GetObject y PutObject trabajan sobre objetos,
        # por eso agregamos /bronze/* al ARN del bucket.
        Resource = "${var.data_bucket_arn}/${var.data_prefix}/*"
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-data-processing-s3-policy"
    Environment = var.environment
    Project     = "data-engineering"
  }
}


# ============================================================
# ASOCIACIÓN POLÍTICA → ROL DE EJECUCIÓN
# ============================================================

# Adjunta la política anterior al rol de procesamiento.
#
# Crear el rol y crear la política por separado NO alcanza:
# esta asociación es la que efectivamente une ambos recursos.
resource "aws_iam_role_policy_attachment" "data_processing_s3" {

  role       = aws_iam_role.data_processing_execution.name
  policy_arn = aws_iam_policy.data_processing_s3.arn
}


# ============================================================
# ROL DE AUDITORÍA
# ============================================================

# Rol destinado a tareas de auditoría y observación.
resource "aws_iam_role" "audit_readonly" {

  name = "${var.environment}-audit-readonly-role"

  # Permite que identidades pertenecientes a nuestra
  # propia cuenta AWS puedan asumir este rol.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"

        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-audit-readonly-role"
    Environment = var.environment
    Project     = "data-engineering"
  }
}


# ============================================================
# PERMISOS DE SOLO LECTURA PARA AUDITORÍA
# ============================================================

# Asociamos al rol de auditoría la política administrada
# ReadOnlyAccess de AWS.
#
# Esto permite consultar recursos sin otorgar permisos
# de creación, modificación o eliminación.
resource "aws_iam_role_policy_attachment" "audit_readonly" {

  role = aws_iam_role.audit_readonly.name

  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}