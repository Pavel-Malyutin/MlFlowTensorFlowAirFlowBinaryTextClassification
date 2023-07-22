FROM tiangolo/uvicorn-gunicorn:python3.9

WORKDIR /repository

COPY . ./repository

COPY requirements.txt /repository/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /repository/requirements.txt

ENV PYTHONUNBUFFERED 1

CMD ["gunicorn", "--bind", ":8080", "repository.app:app", "--worker-class", "uvicorn.workers.UvicornH11Worker", "--timeout", "300", "--max-requests", "100", "--backlog", "2048", "--workers", "2", "--threads", "4", "--log-level", "debug"]