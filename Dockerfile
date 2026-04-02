FROM python:3.9-slim-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    wget \
    procps \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

RUN ln -s $(readlink -f /usr/bin/java | sed "s:bin/java::") /opt/jdk
ENV JAVA_HOME=/opt/jdk

WORKDIR /app/major-etl-app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app/major-etl-app

RUN chmod +x main.py

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    wget \
    procps \
    python3-dev \
    libpq-dev \
    gcc \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*