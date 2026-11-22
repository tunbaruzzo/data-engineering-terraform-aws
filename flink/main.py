
# ============================================================
# PROCESAMIENTO EN TIEMPO REAL - APACHE FLINK
# PRE-ENTREGAS 04 Y 05 - DATA ENGINEERING
# ============================================================

import json
import os
from pathlib import Path


# ============================================================
# CONFIGURACION TEMPRANA DE HADOOP
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

BASE_DIR = Path(__file__).resolve().parent


def configure_hadoop_classpath():
    """
    Preparar Hadoop antes de inicializar la JVM de PyFlink.

    Solo para pruebas locales. En AWS Managed Flink
    el runtime Java puede estar iniciado previamente.

    Esta funcion NO constituye una correccion del
    ClassLoader administrado de AWS.
    """

    if os.environ.get("IS_LOCAL") != "true":
        return

    jar_path = (
        BASE_DIR / "target" / "pyflink-dependencies.jar"
    )

    if not jar_path.is_file():
        raise FileNotFoundError(
            f"No se encontro el JAR: {jar_path}"
        )

    os.environ["HADOOP_CLASSPATH"] = str(jar_path)


# Configurar antes de importar e inicializar PyFlink.
configure_hadoop_classpath()


from pyflink.table import EnvironmentSettings, TableEnvironment


# ============================================================
# CONFIGURACION GENERAL
# ============================================================

AWS_REGION = "us-east-1"

STREAM_ARN = os.environ.get(
    "FLINK_STREAM_ARN",
    "arn:aws:kinesis:us-east-1:000000000000:"
    "stream/data-engineering-dev-ingestion"
)

# La ARN anterior es un placeholder para desarrollo.
# En AWS se obtiene desde propiedades de ejecucion.

WINDOW_SECONDS = 60
WATERMARK_SECONDS = 10


# ============================================================
# OBTENER CONFIGURACION DEL STREAM
# ============================================================

def get_stream_config():

    properties_path = "/etc/flink/application_properties.json"

    if os.path.isfile(properties_path):

        with open(
            properties_path,
            encoding="utf-8"
        ) as file:
            properties = json.load(file)

        for group in properties:

            if group["PropertyGroupId"] == "InputStream0":

                config = group["PropertyMap"]

                return (
                    config["stream.arn"],
                    config["aws.region"]
                )

        raise ValueError("Falta el grupo InputStream0")

    # Configuracion local.
    if "FLINK_STREAM_ARN" not in os.environ:
        raise ValueError(
            "Definir FLINK_STREAM_ARN para pruebas locales"
        )

    return STREAM_ARN, AWS_REGION


# ============================================================
# OBTENER CONFIGURACION DEL LAKEHOUSE
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

def get_lakehouse_config():

    properties_path = "/etc/flink/application_properties.json"

    if os.path.isfile(properties_path):

        with open(
            properties_path,
            encoding="utf-8"
        ) as file:
            properties = json.load(file)

        for group in properties:

            if group["PropertyGroupId"] == "LakehouseConfig":

                config = group["PropertyMap"]

                return (
                    config["catalog.database"],
                    config["catalog.warehouse"],
                    config["aws.region"]
                )

        raise ValueError("Falta el grupo LakehouseConfig")

    # Configuracion local para pruebas.
    database = os.environ.get("GLUE_DATABASE")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE")

    if not database or not warehouse:
        raise ValueError(
            "Definir GLUE_DATABASE e ICEBERG_WAREHOUSE "
            "para pruebas locales"
        )

    return database, warehouse, AWS_REGION


# ============================================================
# APLICACION PRINCIPAL
# ============================================================

def main():

    # Crear el entorno de procesamiento continuo.
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    # Interpretar los horarios en UTC.
    table_env.get_config().set(
        "table.local-time-zone", "UTC"
    )

    # Un unico proceso paralelo para pruebas iniciales.
    table_env.get_config().set(
        "parallelism.default", "1"
    )

    # ========================================================
    # DEPENDENCIAS JAVA PARA EJECUCION LOCAL
    # ========================================================

    if os.environ.get("IS_LOCAL") == "true":

        jar_path = (
            BASE_DIR / "target" / "pyflink-dependencies.jar"
        )

        table_env.get_config().set(
            "pipeline.jars",
            jar_path.as_uri()
        )

    # ========================================================
    # CONFIGURACION DEL STREAM
    # ========================================================

    stream_arn, region = get_stream_config()

    # ========================================================
    # CATALOGO ICEBERG - AWS GLUE DATA CATALOG
    # PRE-ENTREGA 05
    # ========================================================

    database, warehouse, lakehouse_region = (
        get_lakehouse_config()
    )

    # Registrar el catalogo Iceberg respaldado por Glue.
    table_env.execute_sql(f"""
        CREATE CATALOG glue_catalog WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'warehouse' = '{warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'client.region' = '{lakehouse_region}'
        )
    """)

    # Mantener Kinesis en el catalogo temporal de Flink.
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")

    # ========================================================
    # TABLA DE ORIGEN - KINESIS
    # ========================================================

    # El timestamp original llega como ISO-8601 UTC.
    # Se convierte a Event Time utilizando segundos UTC.
    # Las fracciones de segundo se descartan en esta
    # version, como en la Pre-entrega 04.

    table_env.execute_sql(f"""
        CREATE TEMPORARY TABLE transactions (
            transaction_id STRING,
            user_id STRING,
            event_type STRING,
            amount DOUBLE,
            currency STRING,
            `timestamp` STRING,

            event_time AS TO_TIMESTAMP_LTZ(
                UNIX_TIMESTAMP(
                    REPLACE(
                        SUBSTRING(`timestamp`, 1, 19),
                        'T',
                        ' '
                    )
                ) * 1000,
                3
            ),

            WATERMARK FOR event_time AS
                event_time - INTERVAL '{WATERMARK_SECONDS}' SECOND

        ) WITH (
            'connector' = 'kinesis',
            'stream.arn' = '{stream_arn}',
            'aws.region' = '{region}',
            'source.init.position' = 'LATEST',
            'format' = 'json'
        )
    """)

    # ========================================================
    # AGREGACION STATEFUL POR VENTANA
    # ========================================================

    query = f"""
        INSERT INTO glue_catalog.{database}.transaction_metrics

        SELECT
            window_start,
            window_end,
            event_type,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS average_amount

        FROM TABLE(
            TUMBLE(
                TABLE transactions,
                DESCRIPTOR(event_time),
                INTERVAL '{WINDOW_SECONDS}' SECOND
            )
        )

        WHERE event_type IS NOT NULL

        GROUP BY
            window_start,
            window_end,
            event_type
    """

    # ========================================================
    # EJECUCION
    # ========================================================

    result = table_env.execute_sql(query)

    # Evitar que el proceso termine en pruebas locales.
    if os.environ.get("IS_LOCAL") == "true":
        result.wait()


# ============================================================
# PUNTO DE ENTRADA
# ============================================================

if __name__ == "__main__":
    main()
