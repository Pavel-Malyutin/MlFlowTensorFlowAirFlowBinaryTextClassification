import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')

data = pd.read_csv('IMDBDataset.csv')
train_df = data.sample(frac=0.9, random_state=100)

test_df = data.drop(train_df.index)
test_df["label"] = ""
test_df["status"] = "New"

test_df.to_sql("production", engine, if_exists="replace", index_label="id")
train_df.to_sql("train", engine, if_exists="replace", index_label="id")
