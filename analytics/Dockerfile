FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.5 confluent-kafka==2.8.2 psycopg[binary]==3.2.6
WORKDIR /app
COPY analytics analytics
COPY services services
ENV PYSPARK_PYTHON=python
