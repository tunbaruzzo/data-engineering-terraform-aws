
# ============================================================
# PRODUCER SIMULADO - KINESIS DATA STREAMS
# PRE-ENTREGA 02 - DATA ENGINEERING
# ============================================================

import boto3
import json
import random
import time
import uuid

from collections import Counter
from datetime import datetime, timezone

# ============================================================
# CONFIGURACION
# ============================================================

AWS_REGION = "us-east-1"
STREAM_NAME = "data-engineering-dev-ingestion"
TOTAL_EVENTS = 100
MAX_RETRIES = 3

# Cliente AWS Kinesis
kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)

# ============================================================
# GENERACION DE EVENTOS SIMULADOS
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
# ENVIO DE EVENTOS A KINESIS
# ============================================================

def main():

    print("=" * 55)
    print("INICIANDO INGESTA SIMULADA - KINESIS")
    print(f"Stream: {STREAM_NAME}")
    print(f"Eventos a enviar: {TOTAL_EVENTS}")
    print("=" * 55)

    records = []

    for _ in range(TOTAL_EVENTS):

        event = generate_event()

        # Una clave variable favorece la distribucion
        # de los registros entre diferentes shards.
        partition_key = event["transaction_id"]

        records.append({
            "Data": (json.dumps(event) + "\n").encode("utf-8"),
            "PartitionKey": partition_key
        })

    shard_distribution = Counter()
    successful = 0
    pending = records

    # Reintenta solamente los registros rechazados.
    for attempt in range(MAX_RETRIES + 1):

        if not pending:
            break

        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=pending
        )

        failed = []

        # AWS devuelve una respuesta por cada registro
        # respetando el orden de envio.
        for record, result in zip(pending, response["Records"]):

            if "ErrorCode" in result:
                failed.append(record)
            else:
                successful += 1
                shard_distribution[result["ShardId"]] += 1

        pending = failed

        if pending and attempt < MAX_RETRIES:
            wait = 2 ** attempt
            print(
                f"Reintentando {len(pending)} registros "
                f"en {wait} segundos..."
            )
            time.sleep(wait)

    # ========================================================
    # RESULTADOS
    # ========================================================

    print("\nRESULTADO DE INGESTA")
    print("-" * 55)
    print(f"Eventos enviados correctamente: {successful}")
    print(f"Eventos no enviados: {len(pending)}")

    print("\nDISTRIBUCION POR SHARD")

    for shard_id, count in sorted(shard_distribution.items()):
        print(f"{shard_id}: {count} registros")

    print("-" * 55)

    if not pending:
        print("INGESTA COMPLETADA CORRECTAMENTE")
    else:
        print("ATENCION: quedaron registros sin enviar")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
