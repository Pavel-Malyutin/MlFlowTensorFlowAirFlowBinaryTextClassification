## This service is made for binary text classification. 
The are two datasets in this repository: positive and negative reviews and spam or non-spam emails. You can train a model on any of them.

#### You can read more about the model in Collab Notebook:
https://colab.research.google.com/drive/1q7l5gQGBKD2grdBtYGM2EIWmDPZkrTph

## Workflow
* Data is uploaded in DB with status "New", than transfered through Kafka to the model
* During the processing data has status "In progress"
* After getting predictions the data is saved to the database with status "Complete". Than you can download it with "/download/predicted" method

## Services descriptions:

### Repository:
Used for acces to database
##### Endpoints:
* /upload/train To upload data for model training
* /upload/raw To upload data for model predictions
* /download/train To download data for model training
* /upload/predicted To upload predictions
* /download/to_predict To download data for predicting
* /download/predicted To download predicted data

### DAGs in Airflow:
```
Login / password
airflow / airflow
```
#### Train model
Downloads data for a model trianing. Trains the model. Saves ready model to MLFlow

#### Get batches
Downloads data for predictions and sends it to "raw" topic. You can configure batch size

#### Get predictions
Reads data with predictions from topic "predictions" and sends it to the repository

### MlFlow
Model registry. You can find all model versions and choose yours.
All artifacts and models are saved to Minio S3 storage 

### Inference
Service for model predictions. Before starting the container you need to set Run ID in system environments.
The service listens to "raw" topic, gets messages and makes predictions.
Predictions results are uploaded to "predictions" topic.
