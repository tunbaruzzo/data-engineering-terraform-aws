# ============================================================
# VARIABLES DEL MÓDULO DE RED
# ============================================================

# Nombre lógico del entorno que está utilizando este módulo.
# Ejemplos: dev, staging, prod.
# Nos servirá para nombrar y etiquetar los recursos de red.
variable "environment" {
  description = "Nombre del entorno que utiliza el módulo de red"
  type        = string
}

# Región de AWS donde se desplegarán los recursos de red.
# Este valor llega desde environments/dev/main.tf.
variable "aws_region" {
  description = "Región de AWS utilizada por el módulo de red"
  type        = string
}