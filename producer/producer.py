
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")

SENSORS = ["SENSOR_001", "SENSOR_002", "SENSOR_003", "SENSOR_004", "SENSOR_005"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        retries=5,
    )

    print(f"Productor conectado a {BOOTSTRAP_SERVERS}", flush=True)

    try:
        while True:
            sensor_id = random.choice(SENSORS)

            event = {
                "sensor_id": sensor_id,
                "temperature": round(random.uniform(15, 40), 2),
                "humidity": round(random.uniform(30, 90), 2),
                "air_quality_index": random.randint(0, 200),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            future = producer.send(
                TOPIC,
                key=sensor_id,
                value=event,
            )
            metadata = future.get(timeout=10)

            print(
                f"Enviado: {event} | particion={metadata.partition}",
                flush=True,
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("Deteniendo productor...", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
