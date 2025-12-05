# Proyecto de Streaming de Datos en Tiempo Real

## Descripción
Este proyecto implementa un pipeline de streaming de datos en tiempo real desde una API externa. Utiliza Kafka para la ingesta de datos, Airflow para orquestación, Spark para procesamiento y Cassandra para almacenamiento NoSQL.

El flujo principal:
- Recopila datos de la API en tiempo real.
- Los envía a Kafka como topics.
- Airflow programa y monitorea los jobs.
- Spark procesa los datos (e.g., agregaciones, transformaciones).
- Almacena resultados en Cassandra para consultas rápidas.

## Tecnologías Usadas
- **Kafka**: Para mensajería y streaming.
- **Airflow**: Orquestación de workflows.
- **Spark**: Procesamiento distribuido de datos.
- **Cassandra**: Base de datos NoSQL para almacenamiento escalable.
- Otras: Python (para scripts), Docker (para contenedores, opcional).
