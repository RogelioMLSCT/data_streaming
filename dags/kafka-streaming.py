from datetime import datetime
from airflow.sdk import dag, task

# Default arguments for the DAG
deff_arg = {"owner": "RF", "start_date": datetime(2025, 12, 1, 00)}


# define the dag
@dag("user_automation", default_args=deff_arg, schedule="@daily", catchup=False)
def get_random_user_dagflow():
    """DAG to stream random user data to Kafka topic 'user_created' every day for 1 minute."""

    @task()
    def get_data():
        """Fetch random user data from the API."""
        import requests

        res = requests.get("https://randomuser.me/api/")
        res_json = res.json()
        resx = res_json["results"][0]

        return resx

    @task()
    def format_data(res):
        """Format the fetched user data into a structured dictionary."""
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

    @task(task_id="Stream_data_from_api")
    def stream_data(formated_data):
        """Stream formatted user data to Kafka topic 'user_created' for 1 minute."""
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
                res = formated_data

                producer.send("user_created", json.dumps(res).encode("UTF-8"))
            except Exception as e:
                logging.error(f"An error occured: {e}")

                continue

    get_user_data = get_data()
    formated_user_data = format_data(get_user_data)
    streaming_task = stream_data(formated_user_data)
    

# execute the flow
user_automation_dag = get_random_user_dagflow()