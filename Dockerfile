FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Instalar Java (requerido por Apache Spark) y utilidades básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]
