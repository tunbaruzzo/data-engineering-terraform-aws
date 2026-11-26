
# ============================================================
# TABLA APACHE ICEBERG - AWS GLUE DATA CATALOG
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

# Tabla persistente para las metricas procesadas por Flink.
# Glue administra el catalogo e Iceberg guarda los
# datos y metadatos transaccionales en Amazon S3.

resource "aws_glue_catalog_table" "transaction_metrics" {

  name          = "transaction_metrics"
  database_name = aws_glue_catalog_database.lakehouse_db.name

  #description = "Metricas de transacciones agregadas por Flink"

  table_type = "EXTERNAL_TABLE"

  # ==========================================================
  # CONFIGURACION APACHE ICEBERG
  # ==========================================================

  open_table_format_input {

    iceberg_input {

      metadata_operation = "CREATE"
      version            = 2

      iceberg_table_input {

        location = "s3://${var.data_bucket_name}/lakehouse/transaction_metrics/"

        # ====================================================
        # ESQUEMA DE COLUMNAS
        # ====================================================

        schema {
          schema_id = 0
          type      = "struct"

          fields {
            id       = 1
            name     = "window_start"
            type     = jsonencode("timestamp")
            required = true
          }

          fields {
            id       = 2
            name     = "window_end"
            type     = jsonencode("timestamp")
            required = true
          }

          fields {
            id       = 3
            name     = "event_type"
            type     = jsonencode("string")
            required = true
          }

          fields {
            id       = 4
            name     = "transaction_count"
            type     = jsonencode("long")
            required = true
          }

          fields {
            id       = 5
            name     = "total_amount"
            type     = jsonencode("double")
            required = false
          }

          fields {
            id       = 6
            name     = "average_amount"
            type     = jsonencode("double")
            required = false
          }
        }

        # ====================================================
        # PARTICIONADO DIARIO
        # ====================================================

        partition_spec {
          spec_id = 0

          fields {
            field_id  = 1000
            name      = "window_day"
            source_id = 1
            transform = "day"
          }
        }
      }
    }
  }
}
