# ============================================================
# CONSULTA APACHE ICEBERG CON DUCKDB
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

import sys
import duckdb


# Obtener la ubicacion vigente desde el catalogo JDBC.

metadata_location = sys.argv[1]

if not metadata_location.startswith(
    "s3://lakehouse/warehouse/"
):
    raise ValueError("Ubicacion Iceberg local no valida")


# ============================================================
# CONFIGURACION DUCKDB
# ============================================================

con = duckdb.connect(":memory:")

con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")


# ============================================================
# CONEXION EXCLUSIVA A LOCALSTACK
# ============================================================

con.execute("""
    CREATE SECRET localstack_secret (
        TYPE S3,
        KEY_ID 'test',
        SECRET 'test',
        REGION 'us-east-1',
        ENDPOINT 'localhost:4566',
        URL_STYLE 'path',
        USE_SSL false,
        SCOPE 's3://lakehouse/'
    )
""")


# ============================================================
# CONSULTA DE LA TABLA ICEBERG
# ============================================================

print("Tabla: lakehouse_db.transaction_metrics")
print("Metadata:", metadata_location)

print("\nResultados de la consulta:\n")

con.sql("""
    SELECT
        window_start,
        window_end,
        event_type,
        transaction_count,
        total_amount,
        average_amount
    FROM iceberg_scan(?)
    ORDER BY window_start, event_type
""", params=[metadata_location]).show()

con.close()