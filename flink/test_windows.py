
# ============================================================
# PRUEBA LOCAL - VENTANAS TEMPORALES DE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():

    # Entorno de prueba por lotes con datos ficticios.
    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)

    table_env.get_config().set(
        "table.local-time-zone", "UTC"
    )

    # Eventos de prueba con diferentes horarios.
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW test_transactions AS
        SELECT
            event_type,
            amount,
            TO_TIMESTAMP_LTZ(
                event_time_ms, 3
            ) AS event_time
        FROM (
            VALUES
                ('compra', CAST(1000.0 AS DOUBLE),
                 CAST(1762777805000 AS BIGINT)),
                ('compra', CAST(2000.0 AS DOUBLE),
                 CAST(1762777820000 AS BIGINT)),
                ('pago', CAST(500.0 AS DOUBLE),
                 CAST(1762777830000 AS BIGINT)),
                ('compra', CAST(3000.0 AS DOUBLE),
                 CAST(1762777870000 AS BIGINT))
        ) AS events(event_type, amount, event_time_ms)
    """)

    # Crear tabla con atributo de tiempo y watermark.
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW transactions_with_watermark AS
        SELECT
            event_type,
            amount,
            event_time
        FROM test_transactions
    """)

    # Agregacion de ventanas de 60 segundos.
    query = """
        SELECT
            window_start,
            window_end,
            event_type,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS average_amount
        FROM TABLE(
            TUMBLE(
                TABLE transactions_with_watermark,
                DESCRIPTOR(event_time),
                INTERVAL '60' SECOND
            )
        )
        GROUP BY
            window_start,
            window_end,
            event_type
    """


    # ========================================================
    # VALIDACION AUTOMATICA DE RESULTADOS
    # ========================================================

    result = table_env.execute_sql(query)

    # Recuperar las filas calculadas por Flink.
    iterator = result.collect()

    try:
        filas = list(iterator)
    finally:
        iterator.close()

    print("\nRESULTADOS CALCULADOS POR FLINK")
    print("-" * 55)

    obtenidos = []

    for fila in filas:

        inicio, fin, tipo, cantidad, total, promedio = fila

        print(
            f"{inicio} - {fin} | "
            f"{tipo} | "
            f"Cantidad: {cantidad} | "
            f"Total: ${total:.2f} | "
            f"Promedio: ${promedio:.2f}"
        )

        obtenidos.append((
            tipo,
            cantidad,
            round(total, 2),
            round(promedio, 2)
        ))

    # Resultados esperados para los cuatro eventos.
    esperados = [
        ("compra", 2, 3000.0, 1500.0),
        ("pago", 1, 500.0, 500.0),
        ("compra", 1, 3000.0, 3000.0)
    ]

    # El orden de salida de Flink no esta garantizado.
    assert sorted(obtenidos) == sorted(esperados), (
        f"Resultados incorrectos.\n"
        f"Esperados: {esperados}\n"
        f"Obtenidos: {obtenidos}"
    )

    print("-" * 55)
    print("PRUEBA DE AGREGACIONES SUPERADA")
    print("Cantidad, suma y promedio correctos.")



if __name__ == "__main__":
    main()
