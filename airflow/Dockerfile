# Start from the official Airflow image
FROM apache/airflow:2.8.0

# Switch to root user to install system dependencies
USER root

# Install Java (PySpark needs it — same problem we solved on Windows!)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

# Set JAVA_HOME inside the container
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


# Switch back to airflow user for pip installs
USER airflow

# ✅ Copy local file into container — no internet needed
# Install PySpark same version as your Windows install
COPY pyspark-3.5.0.tar.gz /tmp/pyspark-3.5.0.tar.gz
RUN pip install /tmp/pyspark-3.5.0.tar.gz --no-cache-dir

