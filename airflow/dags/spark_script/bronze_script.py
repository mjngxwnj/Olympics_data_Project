from spark_schema import get_schema
from spark_hadoop_io import *

#task
def bronze_task(spark: SparkSession, tables: list, HDFS_load: str):
    df = None
    for table_name in tables:
        '''
            Note that we need to preprocess data for athletes,
            teams and venues table before applying the schema
        '''
        if table_name == 'athletes':
            #read data from csv file
            data = spark.read.csv("/opt/data/athletes.csv", header = True)
            #replace unnecessary character 
            data = data.withColumn("disciplines", regexp_replace("disciplines", "[\[\]']", "")) \
                        .withColumn("events", regexp_replace("events","[\[\]']","")) 
            data = data.withColumn("disciplines", split(data["disciplines"],",")) \
                        .withColumn("events", split(data["events"],",")) \
                        .withColumn("height", col("height").cast("int")) \
                        .withColumn("weight", col("weight").cast("int")) \
                        .withColumn("birth_date", to_date(col("birth_date"), "yyyy-mm-dd")) \
                        .withColumn("lang", split(data["lang"],","))
            #create dataFrame
            df = spark.createDataFrame(data.rdd, schema = get_schema(table_name))

        elif table_name == "teams":
            #read data from csv file
            data = spark.read.csv("/opt/data/teams.csv", header = True)
            #replace unnecessary characters
            data = data.withColumn("athletes", regexp_replace("athletes","[\[\]']","")) \
                    .withColumn("coaches", regexp_replace("coaches","[\[\]']","")) \
                    .withColumn("athletes_codes", regexp_replace("athletes_codes","[\[\]']","")) \
                    .withColumn("coaches_codes", regexp_replace("coaches_codes","[\[\]']",""))
            #transform data type
            data = data.withColumn("athletes", split(data["athletes"],",")) \
                    .withColumn("coaches", split(data["coaches"],",")) \
                    .withColumn("athletes_codes", split(data["athletes_codes"],",")) \
                    .withColumn("coaches_codes", split(data["coaches_codes"],",")) \
                    .withColumn("num_athletes", col("num_athletes").cast("int")) \
                    .withColumn("num_coaches", col("num_coaches").cast("int"))
            #create dataFrame
            df = spark.createDataFrame(data.rdd, schema = get_schema(table_name))

        elif table_name == 'venues':
            #read data from csv file
            data = spark.read.csv("/opt/data/venues.csv", header = True)
            #replace unnecessary characters
            data = data.withColumn("sports", regexp_replace("sports","[\[\]']",""))
            data = data.withColumn("sports", split(data["sports"],",")) \
                    .withColumn("date_start", col("date_start").cast("timestamp")) \
                    .withColumn("date_end", col("date_end").cast("timestamp"))
            #create dataFrame
            df = spark.createDataFrame(data.rdd, schema = get_schema(table_name)) 

        else:
            df = spark.read.csv(f"/opt/data/{table_name}.csv",header = True, schema = get_schema(table_name))

        #After reading csv files or preprocessing, we load data into HDFS
        upload_HDFS(df, table_name, HDFS_load + f'/{table_name}/')

if __name__ == '__main__':
    #list all table
    tables = ['athletes', 'events', 'medallists', 
            'medals', 'schedules_preliminary', 
            'schedules', 'teams', 'torch_route', 'venues']

    #HDFS path
    HDFS_load = "hdfs://namenode:9000/datalake/bronze_storage"

    print("=========================Bronze task starts!========================")
    with get_sparkSession("silver_task_spark") as spark:
        bronze_task(spark, tables, HDFS_load)
    print("========================Bronze task finishes!========================")