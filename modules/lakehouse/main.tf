
# ============================================================
# MODULO LAKEHOUSE - AWS GLUE DATA CATALOG
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

# Base de datos logica para registrar tablas Apache Iceberg.
# Los archivos fisicos de las tablas se almacenaran en S3.

resource "aws_glue_catalog_database" "lakehouse_db" {

  name = "lakehouse_db"

  description = "Catalogo Lakehouse de transacciones procesadas por Flink"

  tags = {
    Name        = "lakehouse_db"
    Environment = var.environment
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }
}


# ============================================================
# UBICACION FISICA DE LAS TABLAS ICEBERG EN S3
# ============================================================

locals {
  iceberg_warehouse_path = "s3://${var.data_bucket_name}/lakehouse/"
}
