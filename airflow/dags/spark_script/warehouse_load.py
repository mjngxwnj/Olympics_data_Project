from spark_hadoop_io import *

def warehouse_task(spark: SparkSession, HDFS_path: str, table_name: str):
    df = read_HDFS(spark, HDFS_path)
    load_snowflake(df, table_name)

if __name__ == "__main__":
    
    #list all table name
    table_name = ['dim_medal', 'dim_discipline', 'dim_event', 
                  'dim_country', 'fact_medallist', 'dim_athletes', 'fact_medal_team',
                  'dim_team', 'fact_schedule', 'dim_venue', 'dim_athletes_team']
    
    #hdfs file to read
    HDFS_read = "hdfs://namenode:9000/datalake/gold_storage"
    
    #loop for calling function
    with get_snowflake_sparkSession("warehouse_task_spark") as spark:
        for name in table_name:
            warehouse_task(spark, HDFS_read + f'/{name}', name)