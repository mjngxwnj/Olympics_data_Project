FROM jupyter/base-notebook:latest

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN pip install pyspark==3.4.3

