FROM python:3.9

WORKDIR /inference

COPY . ./inference

COPY requirements.txt /inference/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /inference/requirements.txt

ENV PYTHONUNBUFFERED 1

CMD [ "python", "inference/predict.py" ]