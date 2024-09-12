from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from sql import create_table, query_report
#set up 


#set default args for DAG
default_args = {
    'owner':'Thuan', 
    'start_date': datetime(2024,7,23),
}

#create DAG
with DAG(
    description = 'process data through bronze, silver, gold layer and store in snowflake datawarehouse',
    dag_id = 'Olympics_data',
    default_args = default_args,    
    schedule_interval = None,
    render_template_as_native_obj = True,
) as dag:
    # '''
    # Bronze task 
    # '''
    # task_bronze = SparkSubmitOperator(
    # task_id = 'bronze_layer_task',
    # application = '/opt/airflow/dags/spark_script/bronze_script.py',
    # conn_id = 'spark_default', 
    # )

    # '''
    #     Silver task
    # '''
    # #create stop_silver_spart task 
    # task_silver = SparkSubmitOperator(
    #     task_id = 'silver_layer_task',
    #     application = '/opt/airflow/dags/spark_script/silver_script.py',
    #     conn_id = 'spark_default', 
    # ) 

    # '''
    #     Check data after silver layer task 
    # '''
    # task_check_data_silver = SparkSubmitOperator(
    #     task_id = 'check_data_silver_layer',
    #     application = '/opt/airflow/dags/spark_script/silverlayer_quality_check.py',
    #     conn_id = 'spark_default', 
    # )

    # '''
    #     Gold task
    # '''
    # #athletes silver task
    # task_gold = SparkSubmitOperator(
    #     task_id = 'gold_layer_task',
    #     application = '/opt/airflow/dags/spark_script/gold_script.py',
    #     conn_id = 'spark_default', 
    # )

    # '''
    #     SQL task
    # '''
    # #create table name
    # SNOWFLAKE_DIM_MEDAL = 'dim_medal'
    # SNOWFLAKE_DIM_DISCIPLINE = 'dim_discipline'
    # SNOWFLAKE_DIM_EVENT = 'dim_event'
    # SNOWFLAKE_DIM_COUNTRY = 'dim_country'
    # SNOWFLAKE_FACT_MEDALLIST = 'fact_medallist'
    # SNOWFLAKE_FACT_MEDAL_TEAM = 'fact_medal_team'
    # SNOWFLAKE_DIM_ATHLETES = 'dim_athletes'
    # SNOWFLAKE_DIM_TEAM = 'dim_team'
    # SNOWFLAKE_DIM_ATHLETES_TEAM = 'dim_athletes_team'
    # SNOWFLAKE_FACT_SCHEDULE = 'fact_schedule'
    # SNOWFLAKE_DIM_VENUE = 'dim_venue'
    # #create table
    # '''
    #     Set airflow task group - create table
    # '''
    # with TaskGroup(group_id = 'create_table') as tg_create_table:
    #     task_dim_medal = SnowflakeOperator(
    #         task_id = 'create_dim_medal',
    #         sql = create_table.create_dim_medal,
    #         params = {'table_name': SNOWFLAKE_DIM_MEDAL},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_dim_discipline = SnowflakeOperator(
    #         task_id = 'create_dim_discipline',
    #         sql = create_table.create_dim_discipline,
    #         params = {'table_name': SNOWFLAKE_DIM_DISCIPLINE},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 
        
    #     task_dim_event = SnowflakeOperator(
    #         task_id = 'create_dim_event',
    #         sql = create_table.create_dim_event,
    #         params = {'table_name': SNOWFLAKE_DIM_EVENT},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_dim_country = SnowflakeOperator(
    #         task_id = 'create_dim_country',
    #         sql = create_table.create_dim_country,
    #         params = {'table_name': SNOWFLAKE_DIM_COUNTRY},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 
        
    #     task_fact_medallist = SnowflakeOperator(
    #         task_id = 'create_fact_medallist',
    #         sql = create_table.create_fact_medallist,
    #         params = {'table_name': SNOWFLAKE_FACT_MEDALLIST},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_fact_medal_team= SnowflakeOperator(
    #         task_id = 'create_fact_medal_team',
    #         sql = create_table.create_fact_medal_team,
    #         params = {'table_name': SNOWFLAKE_FACT_MEDAL_TEAM},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 
        
    #     task_dim_athletes = SnowflakeOperator(
    #         task_id = 'create_dim_athletes',
    #         sql = create_table.create_dim_athletes,
    #         params = {'table_name': SNOWFLAKE_DIM_ATHLETES},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_dim_team = SnowflakeOperator(
    #         task_id = 'create_dim_team',
    #         sql = create_table.create_dim_team,
    #         params = {'table_name': SNOWFLAKE_DIM_TEAM},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_dim_athletes_team = SnowflakeOperator(
    #         task_id = 'create_dim_athletes_team',
    #         sql = create_table.create_dim_athletes_team,
    #         params = {'table_name': SNOWFLAKE_DIM_ATHLETES_TEAM},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_fact_schedule = SnowflakeOperator(
    #         task_id = 'create_fact_schedule',
    #         sql = create_table.create_fact_schedule,
    #         params = {'table_name': SNOWFLAKE_FACT_SCHEDULE},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 

    #     task_dim_venue = SnowflakeOperator(
    #         task_id = 'create_dim_venue',
    #         sql = create_table.create_dim_venue,
    #         params = {'table_name': SNOWFLAKE_DIM_VENUE},
    #         snowflake_conn_id = 'snowflake_default',
    #     ) 
    
    # task_warehouse = SparkSubmitOperator(
    #     task_id = 'warehouse_task',
    #     application = '/opt/airflow/dags/spark_script/warehouse_load.py',
    #     jars = '/opt/jars/snowflake-jdbc-3.19.0.jar,/opt/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar',
    #     conn_id = 'spark_default'
    # )

    #derived table name
    SNOWFLAKE_INDIVIDUAL_MEDAL_COUNT_BY_COUNTRY = 'individual_medal_count_by_country'
    SNOWFLAKE_TEAM_MEDAL_COUNT_BY_COUNTRY = 'team_medal_count_by_contry'
    SNOWFLAKE_TOTAL_MEDAL_COUNT_BY_COUNTRY = 'total_medal_count_by_country'
    SNOWFLAKE_TOTAL_MEDAL_COUNT_BY_AGE = 'total_medal_count_by_age'
    '''
        Set airflow task group - create derived table
    '''
    with TaskGroup(group_id = 'create_derived_table') as tg_create_queried_table:
        task_individual_medal_country = SnowflakeOperator(
            task_id = 'create_individual_medal_country_table',
            sql = query_report.individual_medal_count_by_country,
            params = {'table_name': SNOWFLAKE_INDIVIDUAL_MEDAL_COUNT_BY_COUNTRY},
            snowflake_conn_id = 'snowflake_default'
        )

        task_team_medal_country = SnowflakeOperator(
            task_id = 'create_team_medal_country_table',
            sql = query_report.team_medal_count_by_country,
            params = {'table_name': SNOWFLAKE_TEAM_MEDAL_COUNT_BY_COUNTRY},
            snowflake_conn_id = 'snowflake_default'
        )

        task_total_medal_country = SnowflakeOperator(
            task_id = 'create_total_medal_country_table',
            sql = query_report.total_medal_count_by_country,
            params = {'table_name': SNOWFLAKE_TOTAL_MEDAL_COUNT_BY_COUNTRY},
            snowflake_conn_id = 'snowflake_default'
        )

        task_total_medal_age = SnowflakeOperator(
            task_id = 'create_total_medal_age_table',
            sql = query_report.total_medal_count_by_age,
            params = {'table_name': SNOWFLAKE_TOTAL_MEDAL_COUNT_BY_AGE},
            snowflake_conn_id = 'snowflake_default'
        )
        
        [task_individual_medal_country, task_team_medal_country] >> task_total_medal_country

#task_bronze >> task_silver >> [task_check_data_silver, task_gold]

#[task_gold, tg_create_table] >> task_warehouse >> tg_create_queried_table