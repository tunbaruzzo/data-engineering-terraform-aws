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

  # CIDR principal de la VPC.
  #
  # El valor se define en environments/dev/variables.tf
  # y se envía al módulo network para evitar dejarlo
  # hardcodeado dentro del módulo reutilizable.
  vpc_cidr = var.vpc_cidr
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

# Invoca el módulo ubicado en modules/identity.
#
# Este módulo crea:
# - rol de ejecución para servicios de procesamiento
# - rol de auditoría
#
# Además, recibe información del módulo storage para
# aplicar permisos S3 de mínimo privilegio.
module "identity" {

  # Ruta hacia el módulo de identidad.
  source = "../../modules/identity"

  # Nombre del entorno actual.
  environment = var.environment

  # ARN del bucket S3 creado por el módulo storage.
  #
  # Se utiliza para construir la política IAM del rol
  # de procesamiento sin hardcodear el bucket.
  data_bucket_arn = module.storage.data_bucket_arn

  # Prefijo autorizado dentro del bucket.
  #
  # Debe coincidir con el prefijo definido en el módulo
  # storage para limitar el acceso solamente a esa zona.
  data_prefix = module.storage.data_prefix
}



# ============================================================
# MODULO DE INGESTA - KINESIS
# ============================================================

# Integra el servicio de streaming al entorno DEV.
# Reutiliza el bucket S3 creado por el modulo storage.

module "kinesis" {
  source = "../../modules/kinesis"

  # Entorno actual.
  environment = var.environment

  # Capacidad inicial: 2 shards.
  shard_count = 2

  # Informacion del Data Lake existente.
  data_bucket_arn = module.storage.data_bucket_arn
  data_prefix     = module.storage.data_prefix
}