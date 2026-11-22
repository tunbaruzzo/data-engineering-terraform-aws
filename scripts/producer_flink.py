
# ============================================================
# PRODUCTOR DE EVENTOS PARA APACHE FLINK
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

import boto3
import json
import random
import time
import uuid

from datetime import datetime, timezone
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURACION
# ============================================================

AWS_REGION = "us-east-1"
AWS_PROFILE = "flink-dev"

STREAM_NAME = "data-engineering-dev-ingestion"

TOTAL_EVENTS = 90
INTERVAL_SECONDS = 2

# ============================================================
# GENERACION DE TRANSACCIONES
# ============================================================

def generate_event():

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 50):04d}",
        "event_type": random.choice(
            ["compra", "pago", "devolucion"]
        ),
        "amount": round(random.uniform(100, 50000), 2),
        "currency": "ARS",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ============================================================
# VALIDACION DEL ESQUEMA DE TRANSACCIONES
# ============================================================

def validate_event(event):

    required_fields = {
        "transaction_id",
        "user_id",
        "event_type",
        "amount",
        "currency",
        "timestamp"
    }

    # Validar estructura y campos obligatorios.
    if not isinstance(event, dict):
        return False

    if set(event.keys()) != required_fields:
        return False

    # Validar identificadores y campos de texto.
    text_fields = (
        "transaction_id",
        "user_id",
        "event_type",
        "currency",
        "timestamp"
    )

    for field in text_fields:
        if not isinstance(event[field], str) or not event[field].strip():
            return False

    # Validar valores de negocio.
    if event["event_type"] not in {"compra", "pago", "devolucion"}:
        return False

    if event["currency"] != "ARS":
        return False

    amount = event["amount"]

    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return False

    if not 100 <= amount <= 50000:
        return False

    # Validar fecha ISO-8601 con zona horaria UTC.
    try:
        timestamp = datetime.fromisoformat(event["timestamp"])

        if timestamp.tzinfo is None:
            return False

        if timestamp.utcoffset().total_seconds() != 0:
            return False

    except (ValueError, TypeError):
        return False

    return True


# ============================================================
# ENVIO DE EVENTOS
# ============================================================

def main():

    # Crear la conexion AWS solo al ejecutar el productor.
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION
    )

    kinesis = session.client("kinesis")
    print("INICIANDO PRODUCTOR PARA FLINK")
    print(f"Stream: {STREAM_NAME}")
    print(f"Eventos: {TOTAL_EVENTS}")
    print(f"Intervalo: {INTERVAL_SECONDS} segundos")
    print("-" * 55)

    successful = 0

    for i in range(TOTAL_EVENTS):

        event = generate_event()

        # Validar el evento antes de enviarlo a Kinesis.
        if not validate_event(event):
            raise ValueError(
                f"Evento {i + 1} invalido. "
                "Se cancela el envio a Kinesis."
            )

        try:
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=event["transaction_id"]
            )

            successful += 1

            print(
                f"[{i + 1}/{TOTAL_EVENTS}] "
                f"{event['event_type']} | "
                f"${event['amount']:.2f} | "
                f"{event['timestamp']}"
            )

        except ClientError as error:
            print(f"ERROR EN KINESIS: {error}")
            raise

        if i < TOTAL_EVENTS - 1:
            time.sleep(INTERVAL_SECONDS)

    print("-" * 55)
    print(f"EVENTOS ENVIADOS CORRECTAMENTE: {successful}")


if __name__ == "__main__":
    main()
