import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    # create keyspace here
    session.execute(
        """ 
                    CRETAE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1}
                    """
    )
    print("keyspace created succesfully")


def create_table(session):
    # create table
    session.execute(
        """ CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        adress TEXT,
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
    first_name = kwargs.get("first_name")
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
            INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, adress, postcode, coordinates, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id,
                first_name,
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

        s_conn.SparkContext.setLoglevel("ERROR")
        logging.info("Spark connection created succefully")
    except Exception as r:
        logging.error(f"Couldnt create connection : {r}")

    return s_conn


def create_cassandra_connection():
    # cassandra connection to the cluster
    try:
        cluster = Cluster(["localhost"])

        cas_session = cluster.control_connection

        return cas_session
    except Exception as e:
        logging.error(f"couldnt connect to cassandra cluster due to: {e}")
        return None

    return session


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session is None:
            create_keyspace(session)
            create_table(session)
            insert_data()
