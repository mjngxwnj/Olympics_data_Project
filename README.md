# Data Pipeline for Olympics 2024 Analysis
## Description
This project focuses on building an end-to-end data pipeline that extracts raw data from Kaggle, processes and transforms it across three structured layers (Bronze, Silver, and Gold) using Apache Spark, and stores it in Hadoop HDFS. The final transformed data is then loaded into a Snowflake data warehouse. The entire pipeline is orchestrated using Apache Airflow. For data visualization and reporting, Apache Superset is used to create interactive and informative dashboards.
## Table of contents
- [Architecture](#Architecture)
- [Overview](#Overview)
- [Set up](#Set-up)
  - [Set up Docker](#Set-up-Docker)
  - [Set up Airflow](#Set-up-Airflow)
  - [Run pipeline](#Run-pipeline)
## Architecture
![Architecture](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/Architecture.png)
## Overview
### Directory tree
```
.
├───airflow                # Airflow folder
│   └───dags               # Directory for DAG files (main.py file)
│       ├───spark_script   # Directory for Spark script files for the pipeline
│       └───sql            # Directory for SQL scripts (creating and querying tables)
├───data                   # Data files
├───images                 # Images
├───jars                   # JAR files for configuration between Spark and Snowflake
└───notebook               # Notebooks for Demo data pipeline
```
### Schema
Here is the schema based on snowflake schema model, which includes 3 fact tables and 8 dimensional tables.
This schema will be applied in the data warehouse.
![Schema](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/Snowflake_schema.png)
### Prerequisite
- [Docker](https://www.docker.com/products/docker-desktop)
- [Snowflake account](https://www.snowflake.com/en/data-cloud/platform)
## Set up
### Set up Docker
Clone this project by running the following commands:
```
git clone https://github.com/mjngxwnj/Olympics_data_Project.git
cd Olympics_data_Project
```
After that, run the following command to start Docker Compose (make sure Docker is installed and running):
```
docker-compose up
```
### Set up Airflow
First, go to [localhost:8080](http://localhost:8080), then log in to Airflow with username: `admin`, password: `admin`.
Next, navigate `Admin` -> `Connection`, and edit `spark_default` connection.
The Spark setting should look like this:
![spark_default](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/Airflow_Spark.PNG)
Save the connection, then go back to the DAGs page to prepare for triggering the DAGs.
### Run pipeline
Go to Snowflake account, navigating Database and create Database OLYMPICS_DB, two Schemas OLYMPICS_SCHEMA and REPORT.
![trigger dag](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/Dag.PNG)
To trigger the DAG, click `trigger DAG` in the top right corner. The pipeline will start.
Go to [localhost:9870](http://localhost:9870).
Initially, HDFS will be empty.
![hdfs_empty](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/HDFS.PNG)
As Olympics_data DAG runs, the data will be loaded into HDFS.
![hdfs_finish](https://github.com/mjngxwnj/Olympics_data_Project/blob/master/images/HDFS_finish.PNG)
In each directory listed above, we can see all the tables we loaded.




