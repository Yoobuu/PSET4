FROM python:3.11-slim

# Java para Spark + libs necesarias para Postgres
RUN apt-get update && \
    apt-get install -y openjdk-21-jre gcc python3-dev libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Librerías Python necesarias
RUN pip install --no-cache-dir pyspark psycopg2-binary python-dotenv

WORKDIR /app

COPY . /app


CMD ["bash", "-lc", "sleep infinity"]

