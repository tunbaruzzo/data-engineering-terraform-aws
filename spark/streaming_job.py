
"""
Procesamiento de sensores urbanos en tiempo real.

Kafka -> Spark Structured Streaming -> Promedios por sensor
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Configuración externa
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")
CHECKPOINT_PATH = os.getenv(
    "SPARK_CHECKPOINT_PATH", "/tmp/spark-checkpoints/urban-sensors"
)


def main():

    # 1. Inicializar Spark
    spark = (
        SparkSession.builder
        .appName("UrbanSensorsStreaming")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir el esquema de los eventos JSON
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("air_quality_index", IntegerType(), True),
        StructField("timestamp", StringType(), True),
    ])

    # 3. Leer continuamente los eventos desde Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 4. Interpretar el JSON recibido
    events = (
        kafka_stream
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .filter(
            col("sensor_id").isNotNull()
            & col("event_time").isNotNull()
            & col("temperature").isNotNull()
            & col("air_quality_index").isNotNull()
        )
    )

    # 5. Agrupar por sensor y ventanas de 1 minuto
    averages = (
        events
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("sensor_id"),
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("air_quality_index").alias("avg_air_quality_index"),
            count("*").alias("total_events"),
        )
    )

    # 6. Mostrar los resultados en los logs de Spark
    query = (
        averages.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Spark Structured Streaming iniciado", flush=True)
    print(f"Origen Kafka: {KAFKA_SERVERS}", flush=True)
    print(f"Topico: {KAFKA_TOPIC}", flush=True)

    query.awaitTermination()


if __name__ == "__main__":
    main()
