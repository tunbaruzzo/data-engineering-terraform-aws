
# Imagen oficial de Apache Spark con Python y Java 17
FROM ghcr.io/apache/spark-docker/spark:3.5.7-scala2.12-java17-python3-ubuntu

# Directorio de trabajo de Spark
WORKDIR /opt/spark/work-dir

# Copiar nuestro procesamiento a la imagen
COPY --chown=spark:spark streaming_job.py ./streaming_job.py

# Mantener la ejecución sin privilegios de root
USER spark
