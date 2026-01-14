# Real-Time Data Streaming Project
## Description
This project implements a real-time data streaming pipeline from an external API. It uses Kafka for data ingestion, Airflow for orchestration, Spark for processing, and Cassandra for NoSQL storage.

The main flow:
- Collects data from the API in real time.
- Sends them to Kafka as topics.
- Airflow schedules and monitors the jobs.
- Spark processes the data.
- Stores results in Cassandra for quick queries.

## Technologies Used

- **Kafka**: For messaging and streaming.
- **Airflow**: Workflow orchestration.
- **Spark**: Distributed data processing.
- **Cassandra**: Scalable NoSQL database.
Others: Python (for scripts), Docker (for containers, optional).
