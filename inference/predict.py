import json
import multiprocessing
import os
import pickle

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

    def run_loop(self):
        while True:
            batch = self.__consumer()
            if batch:
                print("Start prediction")
                df = pd.DataFrame.from_records(batch)
                tokenized_text = self.__tokenizer.texts_to_sequences(df["review"])
                data = tf.keras.preprocessing.sequence.pad_sequences(tokenized_text, maxlen=500)
                df["sentiment"] = self.__model.predict(data)
                df = df.drop(["review"], axis=1)
                df.loc[df['sentiment'] > 0, 'sentiment'] = "Negative"
                df.loc[df['sentiment'] != "Negative", 'sentiment'] = "Positive"
                self.__producer.send({"predictions": df.to_json()})
            else:
                print("No data to process")


if __name__ == '__main__':

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_URL", ""))

    # model = Model(run_id=os.environ.get("RUN_ID", ""))
    # model.run_loop()

    workers = multiprocessing.cpu_count() * 2
    processes = []

    for i in range(workers):
        model = Model(run_id=os.environ.get("RUN_ID", ""))
        p = multiprocessing.Process(target=model.run_loop())
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
