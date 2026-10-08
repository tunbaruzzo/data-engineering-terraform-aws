# ============================================================
# INFORMACIÓN DE LA CUENTA AWS
# ============================================================

# Obtiene automáticamente el ID de la cuenta AWS actual.
# No crea ningún recurso.
data "aws_caller_identity" "current" {}


# ============================================================
# BUCKET S3 PARA EL TERRAFORM STATE
# ============================================================

# Crea el bucket donde Terraform almacenará de forma remota
# el archivo terraform.tfstate.
#
# Incorporamos el ID de la cuenta al nombre porque los nombres
# de buckets S3 deben ser únicos globalmente.
resource "aws_s3_bucket" "terraform_state" {

  bucket = "data-engineering-tfstate-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "data-engineering-terraform-state"
    Environment = "shared"
    Project     = "data-engineering"
    Purpose     = "terraform-state"
  }
}


# ============================================================
# VERSIONADO DEL BUCKET
# ============================================================

# Activa el versionado del bucket.
#
# Esto permite conservar versiones anteriores del archivo
# terraform.tfstate y recuperarlas ante un cambio accidental.
resource "aws_s3_bucket_versioning" "terraform_state" {

  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}


# ============================================================
# CIFRADO DEL BUCKET
# ============================================================

# Habilita cifrado del lado del servidor (SSE)
# para proteger el state almacenado en S3.
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {

  bucket = aws_s3_bucket.terraform_state.id

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
resource "aws_s3_bucket_public_access_block" "terraform_state" {

  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# ============================================================
# TABLA DYNAMODB PARA STATE LOCKING
# ============================================================

# Terraform utilizará esta tabla para bloquear el state
# mientras se estén realizando operaciones sobre él.
#
# Esto evita que dos ejecuciones modifiquen simultáneamente
# el mismo estado.
resource "aws_dynamodb_table" "terraform_locks" {

  name         = "data-engineering-terraform-locks"
  billing_mode = "PAY_PER_REQUEST"

  # Clave utilizada para identificar cada bloqueo.
  hash_key = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "data-engineering-terraform-locks"
    Environment = "shared"
    Project     = "data-engineering"
    Purpose     = "terraform-state-locking"
  }
}