# Pre-entrega 1 - Infraestructura Base con Terraform y AWS

## Descripción

Este proyecto implementa la infraestructura base de un entorno de Data Engineering en AWS utilizando Terraform.

La solución fue desarrollada siguiendo un enfoque de Infrastructure as Code (IaC), con separación por módulos y entorno, buscando que la infraestructura sea reproducible, mantenible y reutilizable.

El entorno implementado corresponde a:

- Entorno: `dev`
- Región AWS: `us-east-1`

---

# Objetivos de la pre-entrega

La implementación busca cubrir los siguientes puntos:

- Configurar un backend remoto para Terraform.
- Almacenar el Terraform State en Amazon S3.
- Implementar state locking mediante Amazon DynamoDB.
- Crear una VPC destinada a cargas de Data Engineering.
- Crear al menos dos subredes privadas en distintas Availability Zones.
- Configurar un S3 Gateway Endpoint.
- Implementar roles IAM para procesamiento y auditoría.
- Aplicar permisos de mínimo privilegio sobre el bucket de datos.
- Organizar la infraestructura mediante módulos reutilizables.
- Mantener separados los recursos correspondientes al entorno `dev`.

---

# Arquitectura implementada

```mermaid
flowchart TD

    TF[Terraform]

    TF --> S3STATE[S3 - Terraform State]
    TF --> DDB[DynamoDB - State Locking]

    TF --> VPC[VPC DEV]

    VPC --> SUBA[Private Subnet A]
    VPC --> SUBB[Private Subnet B]

    SUBA --> RT[Private Route Table]
    SUBB --> RT

    RT --> VPCE[S3 Gateway Endpoint]
    VPCE --> S3DATA[S3 Data Lake]

    TF --> IAMPROC[IAM Processing Role]
    TF --> IAMAUDIT[IAM Audit Role]

    IAMPROC --> POLICY[S3 Least Privilege Policy]
    POLICY --> S3DATA