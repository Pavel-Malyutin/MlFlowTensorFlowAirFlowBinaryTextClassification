import datetime as dt

from airflow.models import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


def get_batches(size):
    import os
    import requests
    from kafka import KafkaProducer
    import json

    class Producer:
        def __init__(self):
            self.kafka_host = os.environ.get("KAFKAHOST", "kafka1")
            self.kafka_port = os.environ.get("KAFKAPORT", "9092")
            self.repo_host = os.environ.get("REPO_HOST", "host.docker.internal")
            self.repo_port = os.environ.get("REPO_PORT", "8182")
            self.producer = KafkaProducer(
                bootstrap_servers=f"{self.kafka_host}:{self.kafka_port}",
                value_serializer=lambda m: json.dumps(m).encode('ascii')
            )
            self.topic = "raw"
            self.batch_size = size

        @staticmethod
        def on_success(metadata):
            print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

        @staticmethod
        def on_error(e):
            print(f"Error sending message: {e}")

        def get_data(self):
            data = requests.get(
                url=f"http://{self.repo_host}:{self.repo_port}/download/to_predict/{self.batch_size}").json()
            print(f"Get {len(data)} records from {self.repo_host}:{self.repo_port}")
            return data

        def send(self):
            data = self.get_data()
            while len(data):
                msg = {"batch": data}
                future = self.producer.send(self.topic, msg)
                future.add_callback(self.on_success)
                future.add_errback(self.on_error)
                self.producer.flush()
                data = self.get_data()
            self.producer.close()

    producer = Producer()
    producer.send()


# with DAG(dag_id='get_batches', default_args=args, schedule_interval="* * * * *") as dag:
with DAG(dag_id='get_batches', default_args=args, schedule_interval=None) as dag:
    transfer_from_db_to_kafka = PythonVirtualenvOperator(
        task_id='get_batches',
        python_callable=get_batches,
        dag=dag,
        requirements=['kafka-python'],
        op_kwargs={"size": 100}
    )
    transfer_from_db_to_kafka
