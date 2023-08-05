import datetime
import json
import multiprocessing
import os
import pickle
import time

import mlflow
import pandas as pd
import tensorflow as tf
from kafka import KafkaConsumer, KafkaProducer


class Consumer:
    def __init__(self):
        self.topic = os.getenv("RAW_KAFKA_TOPIC", "raw")
        self.host = os.getenv("KAFKA_HOST", "localhost")
        self.port = os.getenv("KAFKA_PORT", "9092")
        self.consumer = self.__get_consumer()
        self.consumer.subscribe([self.topic])

    def __get_consumer(self):
        return KafkaConsumer(
            bootstrap_servers=[f"{self.host}:{self.port}"],
            group_id="demo-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )

    def __call__(self, *args, **kwargs):
        for message in self.consumer:
            return message.value["batch"]


class Producer:
    def __init__(self):
        self.host = os.getenv("KAFKA_HOST", "localhost")
        self.port = os.getenv("KAFKA_PORT", "9092")
        self.producer = KafkaProducer(
            bootstrap_servers=f"{self.host}:{self.port}",
            value_serializer=lambda m: json.dumps(m).encode('ascii')
        )
        self.topic = "predictions"

    @staticmethod
    def on_success(metadata):
        print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

    @staticmethod
    def on_error(e):
        print(f"Error sending message: {e}")

    def send(self, msg):
        future = self.producer.send(self.topic, msg)
        future.add_callback(self.on_success)
        future.add_errback(self.on_error)
        self.producer.flush()


class Model:
    def __init__(self, run_id: str):
        self.__consumer = Consumer()
        self.__producer = Producer()
        self.__run_id = run_id
        self.__tokenizer = self.__get_tokenizer()
        self.__model = self.__get_model()

    def __get_tokenizer(self):
        tokenizer_uri = mlflow.artifacts.download_artifacts(run_id=self.__run_id)
        with open(f"{tokenizer_uri}/tokenizer.pkl", "rb") as f:
            tokenizer = pickle.load(f)
        return tokenizer

    def __get_model(self):
        model = mlflow.tensorflow.load_model(f'runs:/{self.__run_id}/model')
        return model

    def run_loop(self, start: datetime):
        lifetime = os.environ.get("MODEL_LIFETIME", "10")
        while datetime.datetime.now() - start < datetime.timedelta(minutes=int(lifetime)):
            batch = self.__consumer()
            if batch:
                print("Start prediction")
                df = pd.DataFrame.from_records(batch)
                tokenized_text = self.__tokenizer.texts_to_sequences(df["text"])
                data = tf.keras.preprocessing.sequence.pad_sequences(tokenized_text, maxlen=500)
                df["label"] = self.__model.predict(data)
                df = df.drop(["text"], axis=1)
                df.loc[df['label'] < 0, 'label'] = "No"
                df.loc[df['label'] != "No", 'label'] = "Yes"
                self.__producer.send({"predictions": df.to_json()})
            else:
                print("No data to process")


if __name__ == '__main__':

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_URL", "http://localhost:5000"))
    while True:
        experiments = mlflow.search_experiments(view_type=1, order_by=["last_update_time DESC"])
        if len(experiments) == 0:
            print("No experiments found")
            time.sleep(10)
            continue

        runs = mlflow.search_runs([experiments[0].experiment_id], order_by=["created DESC"])
        if runs.empty:
            print("No runs found")
            time.sleep(10)
            continue
        if len(runs) == 1 and runs.loc[0, 'status'] != 'FINISHED':
            print("It is only one run and it is not in FINISHED status")
            time.sleep(10)
            continue
        if len(runs) > 1 and runs.loc[0, 'status'] != 'FINISHED':
            print("Last run is not in FINISHED status, trying to load another one")
            run_id = None
            for _, run in runs.iterrows():
                if run['status'] == 'FINISHED':
                    run_id = run['run_id']
                    break
            if not run_id:
                print("No runs in FINISHED status")
                time.sleep(10)
                continue
        else:
            run_id = runs.loc[0, 'run_id']

        print("Get run: ", run_id)

        # model = Model(run_id=run_id)
        # model.run_loop()

        workers = multiprocessing.cpu_count() * 2
        processes = []
        start_time = datetime.datetime.now()

        for i in range(workers):
            model = Model(run_id=run_id)
            p = multiprocessing.Process(target=model.run_loop(start=start_time))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
