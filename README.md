# Data Pipeline for Olympics 2024 Analysis
## Description
This project focuses on building an end-to-end data pipeline that extracts raw data from Kaggle, processes and transforms it across three structured layers (Bronze, Silver, and Gold) using Apache Spark, and stores it in Hadoop HDFS. The final transformed data is then loaded into a Snowflake data warehouse. The entire pipeline is orchestrated using Apache Airflow. For data visualization and reporting, Apache Superset is used to create interactive and informative dashboards.
## Table of contents
- [Architecture](#Architecture)
- [Overview](#Overview)
- [Set up](#Set-up)
  - [Set up docker](#Set-up-docker)
  - [Set up airflow](#Set-up-airflow)
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
Clone this project, using:
'''
git clone https://github.com/mjngxwnj/Olympics_data_Project.git
cd Olympics_data_Project
'''


