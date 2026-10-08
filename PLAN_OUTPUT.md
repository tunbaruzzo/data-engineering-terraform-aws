# Terraform Plan & Deployment Evidence

## Proyecto

Pre-entrega 1 - Infraestructura base con entorno de desarrollo e IAM Core.

## Entorno desplegado

- Entorno: `dev`
- Región AWS: `us-east-1`
- Infraestructura gestionada con Terraform
- Backend remoto configurado en Amazon S3
- State locking configurado mediante Amazon DynamoDB

---

## Bootstrap del backend remoto

Se creó previamente la infraestructura necesaria para almacenar y proteger el estado de Terraform.

### Recursos creados

- Bucket S3 para Terraform State
- Versionado habilitado
- Cifrado del lado del servidor habilitado
- Bloqueo de acceso público
- Tabla DynamoDB para state locking

Resultado del despliegue:

```text
Apply complete! Resources: 5 added, 0 changed, 0 destroyed.