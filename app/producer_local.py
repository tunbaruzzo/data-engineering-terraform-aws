# ============================================================
# PRODUCTOR DE EVENTOS - PRE-ENTREGA 05
# PRUEBA CONTROLADA DEL PIPELINE LAKEHOUSE LOCAL
# ============================================================

import json
import uuid
from datetime import datetime, timedelta, timezone

import boto3


# ============================================================
# CONFIGURACION LOCALSTACK
# ============================================================

STREAM_NAME = "data-engineering-local-stream"

client = boto3.client(
    "kinesis",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)


# ============================================================
# VENTANA TEMPORAL DE PRUEBA
# ============================================================

window_start = datetime.now(timezone.utc).replace(
    second=0,
    microsecond=0
) - timedelta(minutes=2)


# ============================================================
# EVENTOS CON RESULTADOS PREDECIBLES
# ============================================================

events = [
    ("PURCHASE", 100.0, 5),
    ("PURCHASE", 200.0, 10),
    ("PURCHASE", 300.0, 15),
    ("REFUND", 50.0, 20),
    ("REFUND", 150.0, 25)
]


def send_event(event_type, amount, timestamp):

    event = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": "local-test-user",
        "event_type": event_type,
        "amount": amount,
        "currency": "ARS",
        "timestamp": timestamp.isoformat()
    }

    response = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey="lakehouse-test"
    )

    print(
        f"{event_type:10} | "
        f"Importe: {amount:7.2f} | "
        f"Shard: {response['ShardId']}"
    )


# ============================================================
# ENVIAR EVENTOS DE LA VENTANA
# ============================================================

for event_type, amount, seconds in events:

    send_event(
        event_type,
        amount,
        window_start + timedelta(seconds=seconds)
    )


# ============================================================
# AVANZAR WATERMARK PARA CERRAR LA VENTANA
# ============================================================

send_event(
    "PURCHASE",
    10.0,
    window_start + timedelta(seconds=80)
)

print("\nVentana evaluada:")
print("Inicio:", window_start.isoformat())
print("Fin:", (window_start + timedelta(seconds=60)).isoformat())

print("\nEventos de prueba enviados: 5")
print("Evento de avance de watermark: 1")
print("Total de eventos enviados: 6")