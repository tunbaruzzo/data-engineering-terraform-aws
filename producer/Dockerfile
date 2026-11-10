
# Imagen base de Python con un sistema Linux reducido
FROM python:3.13-slim

# Evita generar archivos .pyc y permite ver logs inmediatamente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script productor
COPY producer.py .

# Ejecutar con un usuario sin privilegios de administrador
USER 10001

# Comando que inicia el productor
CMD ["python", "producer.py"]
