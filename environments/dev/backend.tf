# ============================================================
# BACKEND REMOTO DE TERRAFORM
# ============================================================

# Este bloque indica dónde se almacenará el Terraform State
# del entorno de desarrollo.
#
# En lugar de guardar terraform.tfstate solamente en la PC,
# Terraform utilizará el bucket S3 creado en el bootstrap.

terraform {
  backend "s3" {

    # Bucket S3 creado previamente en bootstrap/backend.
    # Copiar aquí el nombre exacto mostrado por el output:
    # terraform_state_bucket
    bucket = "data-engineering-tfstate-047719648001"

    # Ruta lógica dentro del bucket donde se guardará
    # el state correspondiente al entorno DEV.
    key = "dev/terraform.tfstate"

    # Región donde existe el bucket.
    region = "us-east-1"

    # Fuerza el uso de cifrado para el archivo de state.
    encrypt = true

    # Tabla DynamoDB creada en el bootstrap.
    # Terraform la utilizará para evitar modificaciones
    # concurrentes sobre el mismo state.
    dynamodb_table = "data-engineering-terraform-locks"
  }
}