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


def get_predictions(size):
    import os
    import requests
    from kafka import KafkaConsumer
    import json

    kafka_host = "kafka1"
    kafka_port = "19092"
    repo_host = os.environ.get("REPO_HOST", "host.docker.internal")
    repo_port = os.environ.get("REPO_PORT", "8182")
    consumer = KafkaConsumer(
        bootstrap_servers=[f"{kafka_host}:{kafka_port}"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

    consumer.subscribe(["predictions"])

    try:
        for message in consumer:
            data = message.value
            print(f"Get {len(data)} records from {repo_host}:{repo_port}")
            requests.post(url=f"http://{repo_host}:{repo_port}/upload/predicted_batch", json=data)
    except Exception as e:
        print(f"Error occurred while consuming messages: {e}")
    finally:
        consumer.close()


# with DAG(dag_id='get_predictions', default_args=args, schedule_interval="* * * * *") as dag:
with DAG(dag_id='get_predictions', default_args=args, schedule_interval=None) as dag:
    transfer_predictions_from_db_to_kafka = PythonVirtualenvOperator(
        task_id='get_batches',
        python_callable=get_predictions,
        dag=dag,
        requirements=['kafka-python'],
        op_kwargs={"size": 100}
    )
    transfer_predictions_from_db_to_kafka
