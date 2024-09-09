from spark_hadoop_io import *


'''
    Check data quality of tables
'''
def check_data(spark: SparkSession, HDFS_path: str, tables: list):
    '''
        Check data of all tables
    '''
    for table_name in tables:
        #read file
        df = read_HDFS(spark, HDFS_path + f'/{table_name}/')
        print(f'Checking data for table "{table_name}"...')
        df.show(5, truncate = False)

        #count the number of null values in each column of the table
        df_checkNull = {col:df.filter(df[col].isNull()).count() for col in df.columns}
        for col, num in df_checkNull.items():
            print(f'Number of null values in column "{col}": {num}')

        #print schema to ensure that all data doesn't have nested structure(array type of struct type)
        df.printSchema()
if __name__ == '__main__':

    #list all tables
    tables = ['athletes_silver', 'events_silver', 'medallists_silver', 
                'medals_silver', 'schedules_pre_silver', 'schedules_silver', 
                'teams_silver', 'torch_route_silver', 'venues_silver']

    #path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage"
    with get_sparkSession("check_quality_data_spark") as spark:
        check_data(spark, HDFS_path, tables)

