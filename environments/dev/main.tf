# ============================================================
# COMPOSICIÓN DEL ENTORNO DEV
# ============================================================

# Este archivo conecta los distintos módulos reutilizables
# que forman la infraestructura del entorno de desarrollo.
#
# Actualmente el entorno DEV estará compuesto por:
# - Network  -> infraestructura de red
# - Storage  -> almacenamiento de datos en S3
# - Identity -> roles y permisos IAM


# ============================================================
# INFORMACIÓN DE LA CUENTA AWS ACTUAL
# ============================================================

# Consulta automáticamente información sobre la cuenta AWS
# con la que Terraform está autenticado.
#
# IMPORTANTE:
# Esto NO crea ningún recurso en AWS.
# Es un "data source", es decir, solamente consulta información
# que ya existe.
data "aws_caller_identity" "current" {}


# ============================================================
# MÓDULO DE RED
# ============================================================

# Invoca el módulo ubicado en modules/network.
#
# Este módulo crea:
# - VPC
# - subredes privadas
# - tabla de rutas privada
# - S3 Gateway Endpoint
module "network" {

  # Ruta relativa desde environments/dev
  # hasta el módulo reutilizable.
  source = "../../modules/network"

  # Nombre del entorno actual.
  environment = var.environment

  # Región AWS utilizada por el módulo.
  aws_region = var.aws_region
}


# ============================================================
# MÓDULO DE STORAGE
# ============================================================

# Invoca el módulo ubicado en modules/storage.
#
# Este módulo crea el bucket S3 que funcionará como
# almacenamiento de datos del proyecto.
module "storage" {

  # Ruta hacia el módulo de almacenamiento.
  source = "../../modules/storage"

  # Nombre del entorno actual.
  environment = var.environment

  # ID de la cuenta AWS actual.
  #
  # Lo obtenemos dinámicamente mediante el data source
  # definido al comienzo de este archivo.
  aws_account_id = data.aws_caller_identity.current.account_id

  # Prefijo autorizado dentro del bucket.
  #
  # Más adelante el rol IAM de procesamiento tendrá
  # permisos únicamente sobre esta zona.
  data_prefix = "bronze"
}


# ============================================================
# MÓDULO DE IDENTIDAD
# ============================================================

# Invoca el módulo de identidad.
#
# Recibe información del módulo storage para poder generar
# políticas IAM específicas sobre el bucket y prefijo
# realmente creados.
module "identity" {

  source = "../../modules/identity"

  # Nombre del entorno.
  environment = var.environment

  # ARN del bucket generado por el módulo storage.
  data_bucket_arn = module.storage.data_bucket_arn

  # Prefijo autorizado dentro del bucket.
  data_prefix = module.storage.data_prefix
}