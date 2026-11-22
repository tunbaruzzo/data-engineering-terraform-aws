# ============================================================
# CONTROL DE RECURSOS STREAMING - ENTORNO DEV
# ============================================================

variable "enable_streaming" {
  description = "Habilita Kinesis y Managed Flink para pruebas controladas."
  type        = bool
  default     = false
}