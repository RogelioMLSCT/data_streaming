from datetime import datetime
from airflow import DAG
from airflow.operators import PythonOperator

deff_arg = {"owner": "RF", "start_date": datetime(2025, 11, 30, 00)}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res_json = res.json()
    resx = res_json["results"][0]

    return resx


def format_data(res):
    data = {}
    location = res["location"]
    data["firstname"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"] = (
        str(location["street"]["number"])
        + " "
        + location["street"]["name"]
        + ", "
        + location["city"]
        + ", "
        + location["state"]
        + ", "
        + location["country"]
    )
    data["postcode"] = location["postcode"]
    data["coordinates"] = (
        f"{location['coordinates']['latitude']}, {location['coordinates']['longitude']}"
    )
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

    current_time = time.time()

    while True:
        if time.time() > current_time + 60:  # 1 minuto
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send("user_created", json.dumps(res).encode("UTF-8"))
        except Exception as e:
            logging.error(f"An error occured: {e}")

            continue


with DAG(
    "user_automation", default_args=deff_arg, schedule="@daily", catchup=False
) as dags:

    streaming_task = PythonOperator(
        task_id="Stream_data_from_api", python_callable=stream_data
    )
