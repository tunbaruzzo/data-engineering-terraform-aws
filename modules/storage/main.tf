# ============================================================
# MÓDULO DE STORAGE - AMAZON S3
# ============================================================

# Este módulo crea el bucket principal de datos del entorno.
#
# A diferencia del bucket utilizado para el Terraform State,
# este bucket estará destinado al almacenamiento de datos
# del pipeline.
#
# En esta primera etapa configuramos:
# - bucket S3
# - versionado
# - cifrado del lado del servidor
# - bloqueo de acceso público


# ============================================================
# BUCKET S3 DE DATOS
# ============================================================

# Crea el bucket principal de datos.
#
# El nombre incluye:
# - el proyecto
# - el entorno
# - el ID de la cuenta AWS
#
# Esto ayuda a garantizar que el nombre sea único globalmente.
resource "aws_s3_bucket" "data_lake" {

  bucket = "data-engineering-${var.environment}-${var.aws_account_id}"

  tags = {
    Name        = "${var.environment}-data-lake"
    Environment = var.environment
    Project     = "data-engineering"
    Purpose     = "data-storage"
  }
}


# ============================================================
# VERSIONADO DEL BUCKET
# ============================================================

# Activa el versionado de objetos.
#
# Si un archivo se modifica o elimina accidentalmente,
# S3 podrá conservar versiones anteriores.
resource "aws_s3_bucket_versioning" "data_lake" {

  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}


# ============================================================
# CIFRADO DEL BUCKET
# ============================================================

# Configura cifrado del lado del servidor.
#
# Todos los objetos almacenados en el bucket quedarán
# cifrados automáticamente utilizando AES256.
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {

  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}


# ============================================================
# BLOQUEO DE ACCESO PÚBLICO
# ============================================================

# Impide que el bucket pueda hacerse público accidentalmente.
#
# Esta configuración protege el bucket incluso si más adelante
# alguien intenta configurar una ACL o política pública.
resource "aws_s3_bucket_public_access_block" "data_lake" {

  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}