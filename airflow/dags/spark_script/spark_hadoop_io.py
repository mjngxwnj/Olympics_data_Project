from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import col, split, regexp_replace, to_date
from contextlib import contextmanager
#create spark Session
@contextmanager
def get_sparkSession(appName: str, master: str = 'local'):
    conf = SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.executor.memory", "2g") \
        .set("spark.executor.cores", "2")
    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    print(f"Successfully create SparkSession with app name: {appName}, master: {master}\n")
    try:
        yield spark
    finally:
        spark.stop()
        print("Spark Sesion has stopped!")

#input HDFS function
def upload_HDFS(dataFrame: DataFrame, table_name: str, HDFS_path: str) -> None:
    print(f'''Starting upload file "{table_name}" into {HDFS_path}...''')
    #check types of parameters
    if not isinstance(dataFrame, DataFrame):
        raise TypeError("data must be a DataFrame!")
    if not isinstance(table_name, str):
        raise TypeError("table name must be a string!")
    if not HDFS_path.startswith("hdfs://namenode:9000/"):
        raise TypeError('HDFS path must start with "hdfs://namenode:9000/"')
    #upload data
    dataFrame.write.parquet(HDFS_path, mode = 'overwrite')
    print("========================================================")
    print(f'''Successfully upload "{table_name}" into {HDFS_path}.''')
    print("========================================================")

#read file from HDFS function
def read_HDFS(spark: SparkSession, HDFS_path: str) -> DataFrame:
    print(f"Starting read file from {HDFS_path}.")
    #check parameters
    if not isinstance(spark, SparkSession):
        raise TypeError("spark must be a Spark Session!")
    if not HDFS_path.startswith("hdfs://namenode:9000/"):
        raise TypeError('HDFS path must start with "hdfs://namenode:9000/"')
    #read file
    data = spark.read.parquet(HDFS_path, header = True)
    return data

#set spark connection with snowflake data warehouse
@contextmanager
def get_snowflake_sparkSession(appName: str, master: str = 'local'):
    conf = SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.executor.memory", "2g") \
        .set("spark.executor.cores", "2") \
        .set("spark.jars","/opt/jars/snowflake-jdbc-3.19.0.jar, \
                           /opt/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar")
    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    print(f"Successfully create SparkSession for Snowflake with app name: {appName}, master: {master}\n")
    try:
        yield spark
    finally:
        spark.stop()
        print("Spark Sesion has stopped!")

#default config for snowflake
sfOptions_default = {
    "sfURL": "https://ae58556.ap-southeast-1.snowflakecomputing.com",
    "sfUser": "HUYNHTHUAN",
    "sfPassword": "********", #hide password
    "sfDatabase": "OLYMPICS_DB",
    "sfSchema": "OLYMPICS_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}
#load data from hdfs to snowflake data warehouse
def load_snowflake(dataFrame: DataFrame, table_name: str, sfOptions: dict = sfOptions_default):
    print(f'''Starting upload {table_name} into snowflake...''')
    #check parameters
    if not isinstance(dataFrame, DataFrame):
        raise TypeError("data must be a DataFrame!")
    if not isinstance(table_name, str):
        raise TypeError("table name must be a string!")
    #upload data
    dataFrame.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
    print("========================================================")
    print(f'''Successfully upload "{table_name}" into SnowFlake.''')
    print("========================================================")

