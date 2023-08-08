# Use an Ubuntu base image
FROM ubuntu:latest

# Set environment variables for Spark and Hadoop versions
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Download and extract Spark
RUN apt-get install -y wget
RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
RUN tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
RUN mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt
RUN rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Set environment variables for Spark and Hadoop paths
ENV SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
COPY etl.py .
COPY config/config.json config/config.json

# Install package with pip
RUN apt-get install -y python3 python3-pip
RUN pip install -r requirements.txt

# Run container as an executable
CMD ["python3", "etl.py"]