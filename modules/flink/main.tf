
# ============================================================
# AMAZON MANAGED SERVICE FOR APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

# Aplicacion administrada para procesamiento stateful
# de transacciones recibidas desde Kinesis Data Streams.

resource "aws_kinesisanalyticsv2_application" "flink" {

  # ==========================================================
  # IDENTIFICACION DE LA APLICACION
  # ==========================================================

  name = local.flink_application_name

  description = "Procesamiento en tiempo real de transacciones simuladas"

  # Version compatible con PyFlink 1.20.
  runtime_environment = "FLINK-1_20"

  # Solo procesamiento streaming.
  application_mode = "STREAMING"

  # Rol IAM especifico de la aplicacion.
  service_execution_role = aws_iam_role.flink_execution.arn

  # Evita arrancar la aplicacion automaticamente.
  # La iniciaremos cuando estemos listos para probar.
  start_application = false

  # ==========================================================
  # CONFIGURACION DE LA APLICACION
  # ==========================================================

  application_configuration {

    # --------------------------------------------------------
    # CODIGO DE LA APLICACION EN S3
    # --------------------------------------------------------

    application_code_configuration {

      code_content_type = "ZIPFILE"

      code_content {
        s3_content_location {

        bucket_arn     = var.data_bucket_arn
        file_key       = var.flink_artifact_key
        object_version = var.flink_artifact_version
        }
      }
    }

    # --------------------------------------------------------
    # PROPIEDADES DE EJECUCION
    # --------------------------------------------------------

    environment_properties {

      # Archivos que AWS ejecutara desde el ZIP.
      property_group {

        property_group_id = "kinesis.analytics.flink.run.options"

        property_map = {
          "python"  = "main.py"
          "jarfile" = "lib/pyflink-dependencies.jar"
        }
      }

      # Configuracion de nuestro stream de entrada.
      property_group {

        property_group_id = "InputStream0"

        property_map = {
          "stream.arn" = var.kinesis_stream_arn
          "aws.region" = var.aws_region
        }
      }
    }

    # ========================================================
    # CONFIGURACION DE APACHE FLINK
    # ========================================================

    flink_application_configuration {

      # ------------------------------------------------------
      # CHECKPOINTING
      # ------------------------------------------------------

      checkpoint_configuration {

        configuration_type = "CUSTOM"

        checkpointing_enabled = true

        # Un checkpoint cada 60 segundos.
        checkpoint_interval = 60000

        # Pausa minima de 5 segundos entre checkpoints.
        min_pause_between_checkpoints = 5000
      }

      # ------------------------------------------------------
      # MONITOREO
      # ------------------------------------------------------

      monitoring_configuration {

        configuration_type = "CUSTOM"

        log_level = "INFO"

        metrics_level = "TASK"
      }

      # ------------------------------------------------------
      # PARALELISMO
      # ------------------------------------------------------

      parallelism_configuration {

        configuration_type = "CUSTOM"

        # Una tarea paralela inicial.
        parallelism = 1

        # Una tarea paralela por KPU.
        parallelism_per_kpu = 1

        # Escalado deshabilitado durante las pruebas.
        auto_scaling_enabled = false
      }
    }
  }

  # ==========================================================
  # CLOUDWATCH LOGGING
  # ==========================================================

  cloudwatch_logging_options {

    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  # ==========================================================
  # ETIQUETAS
  # ==========================================================

  tags = {
    Name        = local.flink_application_name
    Environment = var.environment
    Project     = "data-engineering"
    ManagedBy   = "terraform"
  }

  # Asegura que los permisos esten definidos antes
  # de crear la aplicacion.
  depends_on = [
    aws_iam_role_policy.flink_permissions
  ]
}
