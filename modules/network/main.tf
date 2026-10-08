# ============================================================
# MÓDULO DE RED
# ============================================================

# Este módulo será responsable de crear la infraestructura
# de red base del entorno.
#
# En esta primera etapa vamos a crear:
# - Una VPC
# - Más adelante: subredes privadas
# - Tablas de rutas
# - Gateway Endpoint para S3

# ============================================================
# VPC PRINCIPAL
# ============================================================

# Crea una Virtual Private Cloud (VPC).
# La VPC funciona como una red privada aislada dentro de AWS.
resource "aws_vpc" "main" {

  # Rango de direcciones IP privadas disponible dentro de la VPC.
  # 10.0.0.0/16 permite disponer de una gran cantidad
  # de direcciones privadas para futuras subredes.
  cidr_block = "10.0.0.0/16"

  # Permite utilizar nombres DNS dentro de la VPC.
  enable_dns_support = true

  # Permite que los recursos de la VPC reciban nombres DNS.
  enable_dns_hostnames = true

  # Etiquetas para identificar el recurso.
  tags = {
    Name        = "${var.environment}-data-vpc"
    Environment = var.environment
    Project     = "data-engineering"
  }
}


# ============================================================
# SUBREDES PRIVADAS
# ============================================================

# Primera subred privada.
# Se crea dentro de la VPC definida anteriormente.
resource "aws_subnet" "private_a" {

  # Relaciona esta subred con la VPC principal.
  vpc_id = aws_vpc.main.id

  # Rango IP exclusivo para esta subred.
  cidr_block = "10.0.1.0/24"

  # Primera zona de disponibilidad.
  availability_zone = "${var.aws_region}a"

  # Al ser una subred privada, no asignamos
  # automáticamente IP pública a las instancias.
  map_public_ip_on_launch = false

  # Etiquetas del recurso.
  tags = {
    Name        = "${var.environment}-private-subnet-a"
    Environment = var.environment
    Project     = "data-engineering"
  }
}

# Segunda subred privada.
# Se ubica en otra zona de disponibilidad para mejorar
# la disponibilidad y cumplir con la consigna del proyecto.
resource "aws_subnet" "private_b" {

  # Relaciona esta subred con la misma VPC.
  vpc_id = aws_vpc.main.id

  # Segundo rango IP, distinto al de la primera subred.
  cidr_block = "10.0.2.0/24"

  # Segunda zona de disponibilidad.
  availability_zone = "${var.aws_region}b"

  # Tampoco asigna IP pública automáticamente.
  map_public_ip_on_launch = false

  # Etiquetas del recurso.
  tags = {
    Name        = "${var.environment}-private-subnet-b"
    Environment = var.environment
    Project     = "data-engineering"
  }
}



# ============================================================
# TABLA DE RUTAS PRIVADA
# ============================================================

# Crea una tabla de rutas para las subredes privadas.
# Por ahora no tendrá salida directa a Internet.
# Más adelante, el acceso a S3 se resolverá mediante
# un Gateway Endpoint privado.
resource "aws_route_table" "private" {

  # La tabla de rutas pertenece a la VPC principal.
  vpc_id = aws_vpc.main.id

  # Etiquetas para identificar el recurso.
  tags = {
    Name        = "${var.environment}-private-route-table"
    Environment = var.environment
    Project     = "data-engineering"
  }
}

# ============================================================
# ASOCIACIÓN DE SUBRED PRIVADA A
# ============================================================

# Asocia la primera subred privada con la tabla de rutas.
resource "aws_route_table_association" "private_a" {

  # Subred que será asociada.
  subnet_id = aws_subnet.private_a.id

  # Tabla de rutas que utilizará la subred.
  route_table_id = aws_route_table.private.id
}

# ============================================================
# ASOCIACIÓN DE SUBRED PRIVADA B
# ============================================================

# Asocia la segunda subred privada con la misma tabla de rutas.
resource "aws_route_table_association" "private_b" {

  # Subred que será asociada.
  subnet_id = aws_subnet.private_b.id

  # Tabla de rutas que utilizará la subred.
  route_table_id = aws_route_table.private.id
}



# ============================================================
# S3 GATEWAY ENDPOINT
# ============================================================

# Crea un Gateway Endpoint para Amazon S3.
#
# Este recurso permite que las subredes privadas accedan
# a S3 sin necesidad de salir a Internet ni utilizar
# un NAT Gateway.
resource "aws_vpc_endpoint" "s3" {

  # VPC donde se creará el endpoint.
  vpc_id = aws_vpc.main.id

  # Servicio de AWS al que queremos conectarnos.
  # El nombre del servicio depende de la región.
  service_name = "com.amazonaws.${var.aws_region}.s3"

  # Para S3 utilizamos un Gateway Endpoint.
  vpc_endpoint_type = "Gateway"

  # Asociamos el endpoint a la tabla de rutas privada.
  # Así, las subredes que usan esta tabla podrán llegar
  # a S3 de forma privada.
  route_table_ids = [
    aws_route_table.private.id
  ]

  # Etiquetas del recurso.
  tags = {
    Name        = "${var.environment}-s3-endpoint"
    Environment = var.environment
    Project     = "data-engineering"
  }
}