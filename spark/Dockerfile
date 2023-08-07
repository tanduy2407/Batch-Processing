# Use an Ubuntu base image
FROM ubuntu:latest

# Set environment variables for Spark and Hadoop versions
ENV SPARK_VERSION=3.1.1
ENV HADOOP_VERSION=3.2

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Download and extract Spark
RUN apt-get install -y curl
RUN curl -L https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz | tar xz -C /opt

# Set environment variables for Spark and Hadoop paths
ENV SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin
