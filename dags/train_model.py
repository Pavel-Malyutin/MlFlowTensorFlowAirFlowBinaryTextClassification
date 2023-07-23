import datetime as dt

from airflow.models import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=300),
    'depends_on_past': False,
}


def train(epochs):
    import datetime
    import os
    import pickle
    import requests

    import pandas as pd
    import tensorflow as tf

    import mlflow
    from mlflow import log_artifacts

    repo_host = os.environ.get("REPO_HOST", "host.docker.internal")
    repo_port = os.environ.get("REPO_PORT", "8182")

    os.environ["MLFLOW_URL"] = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://s3:9000")
    os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "sample_key")

    mlflow.set_tracking_uri(os.environ["MLFLOW_URL"])

    def get_tokenizer(data: pd.DataFrame, path: str):
        tokenizer = tf.keras.preprocessing.text.Tokenizer(num_words=8000)
        tokenizer.fit_on_texts(data['review'].values)
        with open(f"{path}/tokenizer.pkl", "wb") as f:
            pickle.dump(tokenizer, f)
        return tokenizer

    def create_model(words_count: int):
        new_model = tf.keras.Sequential([
            tf.keras.layers.Embedding(words_count, 128),
            tf.keras.layers.GlobalAveragePooling1D(),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(1)])

        new_model.compile(optimizer='adam',
                          loss=tf.losses.BinaryCrossentropy(from_logits=True),
                          metrics=['accuracy'])
        return new_model

    with mlflow.start_run() as run:

        data = requests.get(f"http://{repo_host}:{repo_port}/download/train").json()

        df = pd.DataFrame.from_records(data).drop(["id"], axis=1)

        df['sentiment'] = df['sentiment'].map({'positive': 0, 'negative': 1})

        train_df = df.sample(frac=0.8, random_state=100)
        test_df = df.drop(train_df.index)

        mlflow.set_experiment(f"Reviews/{datetime.datetime.now().date()}")

        artifacts_path = "artifacts"
        batch_size = 32

        if not os.path.exists(artifacts_path):
            os.makedirs(artifacts_path)

        tokenizer = get_tokenizer(df, artifacts_path)

        train_seq = tokenizer.texts_to_sequences(train_df["review"])
        test_seq = tokenizer.texts_to_sequences(test_df["review"])

        train_data = tf.keras.preprocessing.sequence.pad_sequences(train_seq, maxlen=100)
        test_data = tf.keras.preprocessing.sequence.pad_sequences(test_seq, maxlen=100)

        train_label = train_df['sentiment'].values
        test_label = test_df['sentiment'].values
        word_index = tokenizer.word_index
        nb_words = len(word_index) + 1

        model = create_model(words_count=nb_words)

        call_back = [tf.keras.callbacks.EarlyStopping(monitor="val_accuracy", patience=2,
                                                      verbose=1, restore_best_weights=True)]

        mlflow.tensorflow.autolog()

        model.fit(train_data, train_label, epochs=epochs, batch_size=batch_size,
                  validation_data=(test_data, test_label),
                  callbacks=call_back)

        log_artifacts(artifacts_path)
        mlflow.tensorflow.log_model(model, "reviews")


with DAG(dag_id='train_model', default_args=args, schedule_interval=None) as dag:
    train_model = PythonVirtualenvOperator(
        task_id='train_model',
        python_callable=train,
        dag=dag,
        python_version=3.9,
        system_site_packages=True,
        requirements=['scikit-learn==1.2.2', 'tensorflow==2.13.0', "mlflow", "pandas", "keras==2.13.1"],
        op_kwargs={"epochs": 10}
    )
    train_model
