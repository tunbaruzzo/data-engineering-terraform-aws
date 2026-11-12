
# ============================================================
# CLOUDWATCH LOGS - APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# ============================================================
# GRUPO DE LOGS
# ============================================================

# Almacena los registros de ejecucion de nuestra
# aplicacion Apache Flink.
resource "aws_cloudwatch_log_group" "flink" {

  name = local.flink_log_group_name

  # Conservamos los registros durante 7 dias
  # para controlar los costos de almacenamiento.
  retention_in_days = 7

  tags = {
    Name        = "${var.environment}-flink-logs"
    Environment = var.environment
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# STREAM DE LOGS
# ============================================================

# Dentro del grupo creamos un stream donde el servicio
# publicara mensajes de ejecucion y errores.
resource "aws_cloudwatch_log_stream" "flink" {

  name = local.flink_log_stream_name

  log_group_name = aws_cloudwatch_log_group.flink.name
}
