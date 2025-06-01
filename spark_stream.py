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
    coordinate = kwargs.get("coordinate")
    id = kwargs.get("id")
    id = kwargs.get("id")


def create_spark_connection():
    # spark connection
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datasax.spark:spark-cassandra-connector_2.13:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5",
            )
            .config("spark.cassandra.connection.host", "localhost")
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
