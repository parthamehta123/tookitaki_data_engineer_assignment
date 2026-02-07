FROM python:3.10-slim

# Install Java 21 (required for Spark on Debian trixie)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY run_streaming.py .

ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0 pyspark-shell"

CMD ["python", "run_streaming.py"]
