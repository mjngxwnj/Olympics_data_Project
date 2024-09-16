from pyspark.sql.types import (StructType, StructField, StringType, 
                               IntegerType, FloatType, ArrayType, 
                               DateType, ByteType, TimestampType)
def get_schema(table_name):
    '''
        Create schema for athletes table
    '''
    #list of columns containing string type
    cols = ['code', 'name', 'name_short', 'name_tv',
            'gender', 'function', 'country_code',
            'country', 'country_full', 'nationality', 
            'nationality_full', 'nationality_code']
    
    cols2 = ['birth_place', 'birth_country', 'residence_place', 
             'residence_country', 'nickname', 'hobbies', 'occupation', 
             'education', 'family']
    
    cols3 = ['coach', 'reason', 'hero', 'influence', 'philosophy', 
             'sporting_relatives', 'ritual', 'other_sports']
    
    #create schema for athletes table
    athletes_schema  = [StructField(col, StringType(), True) for col in cols]
    athletes_schema += [StructField('height', IntegerType(), True),
                       StructField('weight', IntegerType(), True),
                       StructField('disciplines', ArrayType(StringType(),True), True),
                       StructField('events', ArrayType(StringType(),True)),
                       StructField('birth_date', DateType(), True)]
    athletes_schema += [StructField(col, StringType(), True) for col in cols2]
    athletes_schema += [StructField('lang', ArrayType(StringType(),True),True)]
    athletes_schema += [StructField(col, StringType(), True) for col in cols3]
    athletes_schema  = StructType(athletes_schema)

    '''
        Create schema for events table
    '''
    #events table has a suitable schema, so ignore it
    events_schema = None
    
    '''
        Create schema for medallists table 
    '''
    medallists_schema = [StructField("medal_date", DateType(), True),
                         StructField("medal_type", StringType(), True),
                         StructField("medal_code", ByteType(), True)]
    
    #list of columns containing string type
    cols = ['name', 'gender', 'country', 'country_code', 'nationality','team', 
            'team_gender', 'discipline', 'event', 'event_type', 'url_event']
    
    medallists_schema += [StructField(col, StringType(), True) for col in cols]
    medallists_schema += [StructField("birth_date", DateType(), True),
                          StructField("code", StringType(), True)]
    medallists_schema  = StructType(medallists_schema)

    '''
        Create schema for medals table
    '''
    medals_schema = [StructField("medal_type", StringType(),True),
                     StructField("medal_code", ByteType(),True),
                     StructField("medal_date", DateType(), True)]
    
    #list of columns containing string type
    cols = ['name', 'country_code', 'gender', 'discipline',
             'event', 'event_type', 'url_event', 'code']
    
    medals_schema += [StructField(col, StringType(), True) for col in cols]
    medals_schema  = StructType(medals_schema)

    '''
        Create schema for schedules table
    '''
    schedules_schema = [StructField("start_date", TimestampType(), True),
                        StructField("end_date", TimestampType(), True),
                        StructField("day", DateType(), True),
                        StructField("status", StringType(), True),
                        StructField("discipline", StringType(), True),
                        StructField("discipline_code", StringType(), True),
                        StructField("event", StringType(), True),
                        StructField("event_medal", IntegerType(), True)]
    
    #list of columns containing string type 
    cols = ['phase', 'gender', 'event_type', 'venue', 
            'venue_code', 'location_description', 'location_code']
    
    schedules_schema += [StructField(col, StringType(), False) for col in cols]
    schedules_schema += [StructField("url", StringType(), True)]
    schedules_schema  = StructType(schedules_schema)

    '''
        Create schema for schedules_preliminary schemas

    '''
    schedules_pre_schema = [StructField("date_start_utc", TimestampType(), True),
                            StructField("date_end_utc", TimestampType(), True),
                            StructField("estimated", StringType(), True),
                            StructField("estimated_start", StringType(), True),
                            StructField("start_text", StringType(), True),
                            StructField("medal", IntegerType(), True),
                            StructField("venue_code", StringType(), True),
                            StructField("description", StringType(), True)]
    
    #list of columns containing string type - can be nullable
    cols = ['venue_code_other', 'discription_other', 
            'team_1_code', 'team_1', 'team_2_code', 'team_2']
    
    schedules_pre_schema += [StructField(col, StringType(), True) for col in cols]
    schedules_pre_schema += [StructField("tag", StringType(), True),
                             StructField("sport", StringType(), True),
                             StructField("sport_code", StringType(), True),
                             StructField("sport_url", StringType(), True)]
    schedules_pre_schema  = StructType(schedules_pre_schema)

    '''
        Create schema for teams table
    '''
    #list of column containing string type
    cols = ['code', 'team', 'team_gender', 'country', 'country_full', 
            'country_code', 'discipline', 'disciplines_code', 'events']
    
    teams_schema  = [StructField(col, StringType(), True) for col in cols]
    teams_schema += [StructField("athletes", ArrayType(StringType(),True)),
                     StructField("coaches", ArrayType(StringType(),True)),
                     StructField("athletes_codes",ArrayType(StringType(),True)),
                     StructField("num_athletes",IntegerType(),True),
                     StructField("coaches_codes",ArrayType(StringType(),True)),
                     StructField("num_coaches",IntegerType(),True)]
    teams_schema = StructType(teams_schema)

    '''
        Create schema for torch_route table
    '''
    torch_route_schema = [StructField("title", StringType(), True),
                          StructField("city", StringType(), True),
                          StructField("date_start", TimestampType(), True),
                          StructField("date_end", TimestampType(), True),
                          StructField("tag", StringType(), True),
                          StructField("url", StringType(), True),
                          StructField("stage_number", IntegerType(), True)]
    torch_route_schema = StructType(torch_route_schema)

    '''
        Create schema for venues table
    '''

    venues_schema = [StructField("venue", StringType(), True),
                     StructField("sports", ArrayType(StringType(),True),True),
                     StructField("date_start", TimestampType(), True),
                     StructField("date_end", TimestampType(), True),
                     StructField("tag", StringType(), True),
                     StructField("url", StringType(), True)]
    venues_schema = StructType(venues_schema)

    #create dict for mapping schema
    schema = {
        'athletes' : athletes_schema,
        'events' : events_schema,
        'medallists' : medallists_schema,
        'medals' : medals_schema,
        'schedules_preliminary' : schedules_pre_schema,
        'schedules' : schedules_schema,
        'teams' : teams_schema,
        'torch_route' : torch_route_schema,
        'venues' : venues_schema
    }

    return schema[table_name]