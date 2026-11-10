ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.13
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless which \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
