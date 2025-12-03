import logging
from datetime import datetime
from kafka import KafkaProducer
import time
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr


def create_keyspace(session):
    # create keyspace here
    session.execute(
        """ 
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'}
                    """
    )
    print("keyspace created succesfully")


def create_table(session):
    # create table
    session.execute(
        """ CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        firstname TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode TEXT,
        coordinates TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
        )
    """
    )

    print("Table created succesfully!")


def insert_data(session, **kwargs):
    # insertion here
    print("Inserting data.....")

    id = kwargs.get("id")
    firstname = kwargs.get("firstname")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("postcode")
    coordinate = kwargs.get("coordinates")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users (id, firstname, last_name, gender, address, postcode, coordinates, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id,
                firstname,
                last_name,
                gender,
                address,
                postcode,
                coordinate,
                email,
                username,
                dob,
                registered_date,
                phone,
                picture,
            ),
        )
        print("Data inserted successfully!")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")


def create_spark_connection():
    # spark connection
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            # JAR local del conector Cassandra
            .config(
                "spark.jars",
                "/home/rfranco/spark-jars/spark-cassandra-connector-assembly_2.13-3.5.0.jar",
            )
            # Solo Kafka va via spark.jars.packages
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5",
            )
            # Configuración de Cassandra
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.auth.username", "cassandra")
            .config("spark.cassandra.auth.password", "cassandra")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")

        logging.info("Spark connection created succefully")
    except Exception as r:
        logging.error(f"Couldnt create connection : {r}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "user_created")
            .option("startingOffsets", "earliest")
            .load()
        )

        logging.info("Connected to Kafka topic succesfully")
    except Exception as e:
        logging.error(f"Couldnt connect to kafka topic due to: {e}")
        
    return spark_df


def create_cassandra_connection():
    # cassandra connection to the cluster
    try:
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"couldnt connect to cassandra cluster due to: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    # define the schema of the incoming data
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
    )

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("firstname", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False), 
            StructField("address", StringType(), False),
            StructField("postcode", StringType(), False),
            StructField("coordinates", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False)     
            
        ]
    )
    
    sel = spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema).alias("data")).select("data.*").withColumn("id", expr("uuid()")) 
    print(sel)
    
    return sel
    
def validate_kafka_connection(bootstrap_servers="broker:29092", retries=5, delay=2):
    """Verifica si Kafka está accesible desde Spark usando TCP."""
    print(f"Validando conexión a Kafka en {bootstrap_servers} ...")
    
    for attempt in range(1, retries+1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=2000,
                api_version_auto_timeout_ms=2000,
            )
            
            # Kafka responde sin publicar nada
            producer.bootstrap_connected()
            print("Conexión a Kafka exitosa")
            producer.close()
            return True

        except Exception as e:
            print(f"No se pudo conectar a Kafka (intento {attempt}/{retries})")
            print(f"   Error: {e}")
            time.sleep(delay)

    print("No se logró conexión a Kafka después de varios intentos.")
    return False
    
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:

        # Validar conexión a Kafka antes de usar Spark
        if not validate_kafka_connection("localhost:9092"):
            logging.error("Kafka no responde. Abortando ejecución.")
            exit(1)

        # connect to kafka topic with spark
        df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            streaming_query = (
                selection_df.writeStream
                .format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .start()
            )
            
            streaming_query.awaitTermination()
        else:
            logging.error("NO hay conexión a Cassandra, no puedo continuar")
