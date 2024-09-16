from pyspark.sql import DataFrame
from spark_hadoop_io import *
from pyspark.sql.functions import (monotonically_increasing_id, concat, 
                                   lit, substring, rtrim, when, length)

#dim table - medal 
def dim_medal(spark: SparkSession, HDFS_load):
    data = [(1,'Gold Medal'),(2,'Silver Medal'),(3,'Bronze Medal')]

    df_gold = spark.createDataFrame(data, schema = "medal_id int, medal_type string")

    #upload hdfs
    upload_HDFS(df_gold, 'dim_medal', HDFS_load + '/dim_medal/')

#dim table - discipline
def dim_discipline(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/schedules_silver"
    df = read_HDFS(spark, HDFS_path)

    #select
    df_gold = df.select('discipline_code', 'discipline').distinct() \
                .withColumnRenamed('discipline_code', 'discipline_id') \
                .withColumnRenamed('discipline', 'discipline_type')
    
    #upload hdfs
    upload_HDFS(df_gold, 'dim_discipline', HDFS_load + '/dim_discipline/')

#dim table - event
def dim_event(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/events_silver"
    df = read_HDFS(spark, HDFS_path)
    df = df.select('event').distinct()

    #add column
    df = df.withColumn('event_id', monotonically_increasing_id())
    df = df.withColumn('event_id', concat(lit('ev'), col('event_id')))

    #rename
    df_gold = df.withColumnRenamed('event', 'event_type')

    #upload hdfs
    upload_HDFS(df_gold, 'dim_event', HDFS_load + '/dim_event/')
    
#dim table - country
def dim_country(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_athletes_path = "hdfs://namenode:9000/datalake/silver_storage/athletes_silver"
    HDFS_team_path = "hdfs://namenode:9000/datalake/silver_storage/teams_silver"

    df1 = read_HDFS(spark, HDFS_athletes_path)
    df2 = read_HDFS(spark, HDFS_team_path)

    #select
    df_nationality = df1.select('country_code', 'country')
    df_country = df1.select('nationality_code', 'nationality')
    df_team_country = df2.select('country_code', 'country')

    #union column
    df_gold = df_nationality.union(df_country).distinct()
    df_gold = df_gold.union(df_team_country).distinct()

    #rename
    df_gold = df_gold.withColumnRenamed('country_code', 'country_id') \
                     .withColumnRenamed('country', 'country_name')
    
    #upload hdfs
    upload_HDFS(df_gold, 'dim_country', HDFS_load + '/dim_country/')
    
#fact table - medallist
def fact_medallist(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/medals_silver"
    HDFS_discipline_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_discipline"
    HDFS_event_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_event"

    df = read_HDFS(spark, HDFS_path)
    df_discipline = read_HDFS(spark, HDFS_discipline_gold)
    df_event = read_HDFS(spark, HDFS_event_gold)
    
    df = df.filter(length('athletes_id') == 7)
    
    df = df.select('athletes_id', 'medal_type', 'medal_date', 'discipline', 'event')

    #handle table individually
    df = df.withColumn('medallist_id', concat(col('athletes_id'),substring('medal_type',1,1))) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Bronze Medal', '3')) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Silver Medal', '2')) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Gold Medal', '1')) \
           .withColumn('medal_type', col('medal_type').cast('int')) \
           .withColumnRenamed('medal_type', 'medal_id') \

    #joining table
    df = df.join(df_discipline, df['discipline'] == df_discipline['discipline_type'], how = 'left')
    df = df.join(df_event, df['event'] == df_event['event_type'], how = 'left')

    #select
    df_gold = df.select('medallist_id', 'athletes_id', 'medal_id', 
                        'medal_date', 'discipline_id', 'event_id').distinct()
    
    #upload hdfs
    upload_HDFS(df_gold, 'fact_medallist', HDFS_load + '/fact_medallist/')

#dim table - athletes
def dim_athletes(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_athletes_path = "hdfs://namenode:9000/datalake/silver_storage/athletes_silver"
    df = read_HDFS(spark, HDFS_athletes_path)

    #select
    df = df.select('athletes_id', 'full_name', 'gender', 
                   'function', 'country_code', 'nationality_code', 
                   'height', 'weight', 'birth_date').distinct()
    
    #rename
    df_gold = df.withColumnRenamed('country_code', 'country_id') \
                .withColumnRenamed('nationality_code', 'nationality_id')
    
    #upload hdfs
    upload_HDFS(df_gold, 'dim_athletes', HDFS_load + '/dim_athletes/')

#fact table - medal_team
def fact_medal_team(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/medals_silver"
    HDFS_discipline_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_discipline"
    HDFS_event_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_event"

    df = read_HDFS(spark, HDFS_path)
    df_discipline = read_HDFS(spark, HDFS_discipline_gold)
    df_event = read_HDFS(spark, HDFS_event_gold)
    
    df = df.filter(length('athletes_id') > 7)

    df = df.select('athletes_id', 'medal_type', 'medal_date', 'discipline', 'event') \
           .withColumnRenamed('athletes_id', 'team_id')
    
    #handle table individually
    df = df.withColumn('medal_team_id', concat(col('team_id'),substring('medal_type',1,1))) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Bronze Medal', '3')) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Silver Medal', '2')) \
           .withColumn('medal_type', regexp_replace('medal_type', 'Gold Medal', '1')) \
           .withColumn('medal_type', col('medal_type').cast('int')) \
           .withColumnRenamed('medal_type', 'medal_id') \

    #joining table
    df = df.join(df_discipline, df['discipline'] == df_discipline['discipline_type'], how = 'left')
    df = df.join(df_event, df['event'] == df_event['event_type'], how = 'left')

    #select
    df_gold = df.select('medal_team_id', 'team_id', 'medal_id', 
                        'medal_date', 'discipline_id', 'event_id').distinct()
    
    #upload hdfs
    upload_HDFS(df_gold, 'fact_medal_team', HDFS_load + '/fact_medal_team/')
    
#dim table - team
def dim_team(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/teams_silver"

    df = read_HDFS(spark, HDFS_path)

    df_gold = df.select('team_id', 'team_name', 'team_gender', 'country_code').distinct() \
                .withColumnRenamed('country_code', 'country_id')
    
    #upload hdfs
    upload_HDFS(df_gold, 'dim_team', HDFS_load + '/dim_team/')

#dim table - athletes-team (associating dimension table)
def dim_athletes_team(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_path = "hdfs://namenode:9000/datalake/silver_storage/teams_silver"

    df = read_HDFS(spark, HDFS_path)

    df_gold = df.select('athletes_id', 'team_id')

    upload_HDFS(df_gold, 'dim_athletes_team', HDFS_load + '/dim_athletes_team/')

#fact table - schedule & dim table - venue
def fact_schedule_dim_venue(spark: SparkSession, HDFS_load):
    #hdfs path
    HDFS_schedule_path = "hdfs://namenode:9000/datalake/silver_storage/schedules_silver"
    HDFS_venue_path = "hdfs://namenode:9000/datalake/silver_storage/venues_silver"
    HDFS_discipline_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_discipline"
    HDFS_event_gold = "hdfs://namenode:9000/datalake/gold_storage/dim_event"

    df = read_HDFS(spark, HDFS_schedule_path)
    df_discipline = read_HDFS(spark, HDFS_discipline_gold)
    df_event = read_HDFS(spark, HDFS_event_gold)
    df_venue = read_HDFS(spark, HDFS_venue_path)
    
    '''
        Create fact - schedule table
    '''
    df = df.withColumn('schedule_id', monotonically_increasing_id())
    df = df.withColumn('schedule_id', concat(lit('sched'), col('schedule_id'))) \
    
    #drop column
    df = df.drop('event_type')

    #joining table
    df = df.join(df_discipline, df['discipline'] == df_discipline['discipline_type'], how = 'left')
    df = df.join(df_event, df['event'] == df_event['event_type'], how = 'left')

    #select
    df_gold_schedule = df.select('schedule_id', 'start_date', 'end_date', 
                        'gender', 'discipline_id', 'phase', 'venue_code', 'event_id') \
                         .withColumnRenamed('venue_code', 'venue_id')
    
    #upload hdfs
    upload_HDFS(df_gold_schedule, 'fact_schedule', HDFS_load + '/fact_schedule/')

    '''
        Create dim - venue table
    '''
    #get data
    df_venue_join = df.select('venue_code', 'venue') \
                      .withColumnRenamed('venue', 'venue_join')
    
    #joining table
    df_venue = df_venue.join(df_venue_join, df_venue['venue'] == df_venue_join['venue_join'], how = 'left')

    #select
    df_gold_venue = df_venue.select('venue_code', 'venue', 'date_start', 'date_end').distinct() \
                       .withColumnRenamed('venue_code', 'venue_id') \
                       .withColumnRenamed('venue', 'venue_name')
    
    #upload hdfs
    upload_HDFS(df_gold_venue, 'dim_venue', HDFS_load + '/dim_venue/')

#main call
def gold_task(spark, HDFS_load):
    #all func
    dim_medal(spark, HDFS_load)
    dim_discipline(spark, HDFS_load)
    dim_event(spark, HDFS_load)
    dim_country(spark, HDFS_load)
    fact_medallist(spark, HDFS_load)
    dim_athletes(spark, HDFS_load)
    fact_medal_team(spark, HDFS_load)
    fact_schedule_dim_venue(spark, HDFS_load)
    dim_team(spark, HDFS_load)
    dim_athletes_team(spark, HDFS_load)

if __name__ == '__main__':
    #hdfs path
    HDFS_load = "hdfs://namenode:9000/datalake/gold_storage"

    print("=========================Gold task starts!========================")
    with get_sparkSession("gold_task_spark", "local") as spark:
        gold_task(spark, HDFS_load)
    print("========================Gold task finishes!========================")