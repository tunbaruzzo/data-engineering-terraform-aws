
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

session = boto3.Session(
    profile_name=AWS_PROFILE,
    region_name=AWS_REGION
)

kinesis = session.client("kinesis")


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
# ENVIO DE EVENTOS
# ============================================================

def main():

    print("INICIANDO PRODUCTOR PARA FLINK")
    print(f"Stream: {STREAM_NAME}")
    print(f"Eventos: {TOTAL_EVENTS}")
    print(f"Intervalo: {INTERVAL_SECONDS} segundos")
    print("-" * 55)

    successful = 0

    for i in range(TOTAL_EVENTS):

        event = generate_event()

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
