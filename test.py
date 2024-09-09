from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("MySparkApp") \
        .master("local")\
        .config('spark.jars','./jars/snowflake-jdbc-3.19.0.jar, ./jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar')\
        .getOrCreate()
sfOptions = {
    "sfURL": "https://ae58556.ap-southeast-1.snowflakecomputing.com",
    "sfUser": "HUYNHTHUAN",
    "sfPassword": "Thuan0355389551",
    "sfDatabase": "OLYMPICS_DB",
    "sfSchema": "OLYMPICS_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}
df = spark.read.csv('D:/Olympics_project/test.csv')
df.show()
df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_MEDAL") \
    .mode("overwrite") \
    .save()