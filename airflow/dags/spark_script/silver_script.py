from silver_layer import Silverlayer
from spark_hadoop_io import *
from pyspark.sql.functions import arrays_zip, explode, col, rtrim, when, ltrim

'''
    Processing tables individually
'''
#athletes
def athletes_silver(spark: SparkSession, HDFS_load):
    '''
        Process athletes table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/athletes"
    df = read_HDFS(spark, HDFS_path)
    #process
    columns_drop = ['name_short', 'name_tv', 'hobbies', 'occupation', 'education', 
                    'family', 'coach', 'reason', 'hero', 'influence', 'philosophy', 
                    'sporting_relatives', 'ritual', 'other_sports']
    df_silver = Silverlayer(df = df, 
                            columns_drop    =  columns_drop,
                            columns_rename  = {'code':'athletes_id', 'name':'full_name', 'lang':'language'},
                            nested_columns  = ['disciplines', 'events', 'language'],
                            missval_columns = {'birth_place':'N/A', 
                                                'birth_country':'N/A', 
                                                'nickname': 'N/A',
                                                'residence_place':'N/A', 
                                                'residence_country':'N/A',
                                                'height':0, 'weight':0}).process()
    #upload hdfs
    upload_HDFS(df_silver, "athletes_silver", HDFS_load + '/athletes_silver/')
#events
def events_silver(spark: SparkSession, HDFS_load):
    '''
        Process events table
    '''
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/events"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df, 
                            columns_dropDuplicates = ['event', 'sport'],
                            columns_drop           = ['sport_url', 'tag'], 
                            columns_rename         = {'sport_code':'id_sport'}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'events_silver', HDFS_load + '/events_silver/')

#medallist
def medallists_silver(spark: SparkSession, HDFS_load):
    '''
        Process medallists table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/medallists"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df, 
                            dropna_columns         = ['name'], 
                            columns_dropDuplicates = ['name', 'medal_type', 'discipline', 'event'],
                            columns_drop           = ['medal_code', 'url_event'],
                            columns_rename         = {'name':'full_name', 'code':'athletes_id'},
                            missval_columns        = {'team':'N/A','team_gender':'N/A', 'nationality':'N/A'}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'medallists_silver', HDFS_load + '/medallists_silver/')

#medals
def medals_silver(spark: SparkSession, HDFS_load):
    '''
        Process medals table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/medals"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df,
                            dropna_columns         = ['name'], 
                            columns_dropDuplicates = ['name', 'medal_type', 'discipline', 'event'],
                            columns_drop           = ['medal_code', 'url_event'],
                            columns_rename         = {'name':'full_name', 'code':'athletes_id'}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'medals_silver', HDFS_load + '/medals_silver/')

#schedules
def schedules_silver(spark: SparkSession, HDFS_load):
    '''
        Process schedules table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/schedules"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df,
                            dropna_columns         = ['event'],
                            columns_dropDuplicates = ['event', 'discipline', 'phase'],
                            columns_drop           = ['status', 'event_medal', 'url']).process()
    #preprocess to match name of venue before joining 
    df_silver = df_silver.withColumn('venue', regexp_replace("venue", "\\d", "")) \
                         .withColumn('venue', rtrim('venue')) \
                         .withColumn('venue_code', regexp_replace("venue_code", "\\d", "")) \
                         .withColumn('venue_code', rtrim('venue_code'))
    df_silver = df_silver.withColumn('venue', when(col('venue') == 'Chateauroux Shooting Ctr', 'Chateauroux Shooting Centre').otherwise(col('venue'))) \
                         .withColumn('venue', when(col('venue') == 'Nautical St - Flat water', 'Vaires-sur-Marne Nautical Stadium').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'BMX Stadium', 'Saint-Quentin-en-Yvelines BMX Stadium').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'Champ-de-Mars Arena', 'Champ de Mars Arena').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'Le Bourget Climbing Venue', 'Le Bourget Sport Climbing Venue').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'Nautical St - White water', 'Vaires-sur-Marne Nautical Stadium').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'Roland-Garros Stadium', 'Stade Roland-Garros').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'Le Golf National', 'Golf National').otherwise(col('venue')))\
                         .withColumn('venue', when(col('venue') == 'National Velodrome', 'Saint-Quentin-en-Yvelines Velodrome').otherwise(col('venue')))
    #upload hdfs
    upload_HDFS(df_silver, 'schedules_silver', HDFS_load + '/schedules_silver/')

#schedules_preliminary
def schedules_pre_silver(spark: SparkSession, HDFS_load):
    '''
        Process schedules preliminary table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/schedules_preliminary"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df,
                            dropna_columns         = ['team_1_code', 'team_2_code', 'team_1', 'team_2'],
                            columns_dropDuplicates = ['team_1_code', 'team_2_code', 'sport'],
                            columns_drop           = ['estimated', 'estimated_start', 'start_text', 
                                                    'medal', 'sport_url', 'tag'],
                            missval_columns        = {'venue_code':'N/A', 'venue_code_other':'N/A',
                                                    'discription_other':'N/A'}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'schedules_pre_silver', HDFS_load + '/schedules_pre_silver/')

#teams
def teams_silver(spark: SparkSession, HDFS_load):
    '''
        Process teams table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/teams"
    df = read_HDFS(spark, HDFS_path)
    #process
    #first, we need to merge array athletes and athletes_code to handle nested structure
    df = df.withColumn('athletes_id_merge', arrays_zip('athletes','athletes_codes'))
    df_silver = Silverlayer(df = df,
                            dropna_columns         = ['code', 'team'],
                            columns_drop           = ['coaches', 'coaches_codes', 'num_coaches', 
                                                      'athletes', 'athletes_codes'],
                            columns_rename         = {'events':'event', 'code':'team_id', 'team':'team_name'},
                            missval_columns        = {'event':'Default'}).process()
    #After silver processing, we process nested structure
    df_silver = df_silver.withColumn('athletes_id_merge', explode('athletes_id_merge'))
    df_silver = df_silver.withColumn('athletes', col('athletes_id_merge.athletes')) \
                         .withColumn('athletes_id', col('athletes_id_merge.athletes_codes'))
    df_silver = df_silver.withColumn('athletes', ltrim('athletes')) \
                         .withColumn('athletes_id', ltrim('athletes_id'))
    df_silver = df_silver.drop('athletes_id_merge')
    #upload hdfs
    upload_HDFS(df_silver, 'teams_silver', HDFS_load + '/teams_silver/')

#torch_route
def torch_route_silver(spark: SparkSession, HDFS_load):
    '''
        Process torch route table
    '''
    #read filey
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/torch_route"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df, 
                            dropna_columns         = ['title'],
                            columns_dropDuplicates = ['title'],
                            columns_drop           = ['tag', 'url'],
                            missval_columns        = {'city':'N/A', 'stage_number':0}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'torch_route_silver', HDFS_load + '/torch_route_silver/')

#venues
def venues_silver(spark: SparkSession, HDFS_load):
    '''
        Process venues table
    '''
    #read file
    HDFS_path = "hdfs://namenode:9000/datalake/bronze_storage/venues"
    df = read_HDFS(spark, HDFS_path)
    #process
    df_silver = Silverlayer(df = df,
                            dropna_columns         = ['venue'],
                            columns_dropDuplicates = ['venue'],
                            columns_drop           = ['tag', 'url'],
                            columns_rename         = {'sports':'sport'},
                            nested_columns         = ['sport'],
                            missval_columns        = {'date_start':'N/A', 'date_end':'N/A'}).process()
    #upload hdfs
    upload_HDFS(df_silver, 'venues_silver', HDFS_load + '/venues_silver/')

def silver_task(spark, HDFS_load):
    athletes_silver(spark, HDFS_load)
    events_silver(spark, HDFS_load)
    medallists_silver(spark, HDFS_load)
    medals_silver(spark, HDFS_load)
    schedules_pre_silver(spark, HDFS_load)
    schedules_silver(spark, HDFS_load)
    teams_silver(spark, HDFS_load)
    torch_route_silver(spark, HDFS_load)
    venues_silver(spark, HDFS_load)

if __name__ == '__main__':
    #hdfs path 
    HDFS_load = "hdfs://namenode:9000/datalake/silver_storage"
    print("=========================Silver task starts!========================")
    with get_sparkSession("silver_task_spark") as spark:
        silver_task(spark, HDFS_load)
    print("========================Silver task finishes!========================")