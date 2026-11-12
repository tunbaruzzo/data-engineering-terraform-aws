
# ============================================================
# PROCESAMIENTO EN TIEMPO REAL - APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

import json
import os

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
# En AWS se obtendra desde propiedades de ejecucion.

WINDOW_SECONDS = 60
WATERMARK_SECONDS = 10


# ============================================================
# OBTENER CONFIGURACION DEL STREAM
# ============================================================

def get_stream_config():

    properties_path = "/etc/flink/application_properties.json"

    if os.path.isfile(properties_path):
        with open(properties_path, encoding="utf-8") as file:
            properties = json.load(file)

        for group in properties:
            if group["PropertyGroupId"] == "InputStream0":
                config = group["PropertyMap"]
                return (
                    config["stream.arn"],
                    config["aws.region"]
                )

        raise ValueError("Falta el grupo InputStream0")

    # Valores locales para la etapa de desarrollo.
    if "FLINK_STREAM_ARN" not in os.environ:
        raise ValueError(
            "Definir FLINK_STREAM_ARN para pruebas locales"
        )

    return STREAM_ARN, AWS_REGION


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

    # Un unico proceso paralelo para las pruebas iniciales.
    table_env.get_config().set(
        "parallelism.default", "1"
    )

    # Cargar el conector Kinesis para ejecucion local.
    if os.environ.get("IS_LOCAL") == "true":
        jar_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "target",
            "pyflink-dependencies.jar"
        )

        table_env.get_config().set(
            "pipeline.jars",
            "file:///" + jar_path.replace("\\", "/")
        )

    stream_arn, region = get_stream_config()

    # ========================================================
    # TABLA DE ORIGEN - KINESIS
    # ========================================================

    # El timestamp original llega como texto ISO-8601 UTC.
    # Se convierte a Event Time usando segundos UTC.
    # En esta primera version se descartan las fracciones
    # de segundo. El productor actual siempre utiliza UTC.

    table_env.execute_sql(f"""
        CREATE TABLE transactions (
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
    # TABLA DE RESULTADOS - DIAGNOSTICO
    # ========================================================

    table_env.execute_sql("""
        CREATE TABLE transaction_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            event_type STRING,
            transaction_count BIGINT,
            total_amount DOUBLE,
            average_amount DOUBLE

        ) WITH (
            'connector' = 'print'
        )
    """)

    # ========================================================
    # AGREGACION STATEFUL POR VENTANA
    # ========================================================

    query = f"""
        INSERT INTO transaction_metrics

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

        GROUP BY
            window_start,
            window_end,
            event_type
    """

    # Iniciar la ejecucion del procesamiento.
    result = table_env.execute_sql(query)

    # Evitar que termine el proceso en pruebas locales.
    if os.environ.get("IS_LOCAL") == "true":
        result.wait()


# ============================================================
# PUNTO DE ENTRADA
# ============================================================

if __name__ == "__main__":
    main()
