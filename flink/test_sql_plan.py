
# ============================================================
# VALIDACION DEL PLAN SQL - APACHE FLINK 1.20
# PRE-ENTREGA 04 - DATA ENGINEERING
#
# No ejecuta el job ni se conecta a AWS.
# ============================================================

from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():

    jar = (
        Path(__file__).resolve().parent
        / "target"
        / "pyflink-dependencies.jar"
    )

    if not jar.is_file():
        raise FileNotFoundError(f"No existe el JAR: {jar}")

    env = TableEnvironment.create(
        EnvironmentSettings.in_streaming_mode()
    )

    env.get_config().set("pipeline.jars", jar.as_uri())
    env.get_config().set("table.local-time-zone", "UTC")

    # Fuente con la estructura REAL de nuestro productor.
    # El ARN es ficticio para realizar la validacion local.
    env.execute_sql("""
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
                event_time - INTERVAL '10' SECOND

        ) WITH (
            'connector' = 'kinesis',
            'stream.arn' =
                'arn:aws:kinesis:us-east-1:000000000000:stream/test',
            'aws.region' = 'us-east-1',
            'source.init.position' = 'LATEST',
            'format' = 'json'
        )
    """)

    env.execute_sql("""
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

    query = """
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
                INTERVAL '60' SECOND
            )
        )

        GROUP BY
            window_start,
            window_end,
            event_type
    """

    print("VALIDANDO PLAN SQL DE FLINK...")

    plan = env.explain_sql(query)

    print(plan)
    print("\nPLAN SQL VALIDADO CORRECTAMENTE")


if __name__ == "__main__":
    main()
