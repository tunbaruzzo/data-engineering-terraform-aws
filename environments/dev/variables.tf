# ============================================================
# VARIABLES DEL ENTORNO DEV
# ============================================================

# Nombre lógico del entorno.
# Lo vamos a reutilizar para nombrar y etiquetar recursos.
# Ejemplos posibles: dev, staging, prod.
variable "environment" {
  description = "Nombre del entorno de despliegue"
  type        = string
  default     = "dev"
}

# Región de AWS donde se crearán los recursos.
# El curso trabaja principalmente con us-east-1, por eso
# la usamos como valor por defecto.
variable "aws_region" {
  description = "Región de AWS donde se desplegará la infraestructura"
  type        = string
  default     = "us-east-1"
}