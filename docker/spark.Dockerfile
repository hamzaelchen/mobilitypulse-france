# One image for Spark master, worker and the Bronze driver, so the Python version
# (3.11) is identical everywhere. PySpark's pip package ships the Spark distribution.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.7
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# requests + confluent-kafka are only needed so the whole test suite can run in this image.
# numpy (1.x, as PySpark 3.5 expects) is required by pyspark.ml (phase 5).
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} numpy==1.26.4 pytest==8.4.2 \
    requests==2.32.5 confluent-kafka==2.11.1

# Kafka connector jars (versions match the dependencies of Spark ${SPARK_VERSION}).
RUN M=https://repo1.maven.org/maven2 \
    && cd ${SPARK_HOME}/jars \
    && curl -fsSLO ${M}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    && curl -fsSLO ${M}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    && curl -fsSLO ${M}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
    && curl -fsSLO ${M}/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar

RUN useradd --create-home --uid 1000 spark && mkdir -p /app /data && chown spark /data
WORKDIR /app
USER spark
