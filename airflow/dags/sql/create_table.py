
create_dim_medal = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        medal_id INT PRIMARY KEY,
        medal_type VARCHAR(20)
    );
'''

create_dim_discipline = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        discipline_id VARCHAR(4) PRIMARY KEY,
        discipline_type VARCHAR(50) 
    );
'''

create_dim_event = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name}} (
        event_id VARCHAR(6) PRIMARY KEY,
        event_type VARCHAR(50)
    );
'''

create_dim_country = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        country_id VARCHAR(5) PRIMARY KEY,
        country_name VARCHAR(20)
    );
'''

create_fact_medallist = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        medallist_id VARCHAR(15) PRIMARY KEY,
        athletes_id VARCHAR(15),
        medal_id SMALLINT,
        discipline_id VARCHAR(4),
        event_id VARCHAR(6)
    );
'''

create_dim_athletes = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        athletes_id VARCHAR(15) PRIMARY KEY,
        full_name VARCHAR(50),
        gender VARCHAR(7),
        function VARCHAR(20),
        country_id VARCHAR(5),
        nationality_id VARCHAR(5),
        height INT,
        weight INT,
        birth_date DATE,
        team_id VARCHAR(25)
    );
'''

create_dim_team = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
        team_id VARCHAR(25) PRIMARY KEY,
        team_name VARCHAR(35),
        team_gender CHAR(1)
    )
'''

create_fact_schedule = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name}} (
        schedule_id VARCHAR(10) PRIMARY KEY,
        start_date DATETIME,
        end_date DATETIME,
        gender CHAR(1),
        discipline_id VARCHAR(4),
        venue_id VARCHAR(4),
        event_id VARCHAR(6)
    );
'''

create_dim_venue = '''
    CREATE TABLE IF NOT EXISTS {{ params.table_name}} (
        venue_id VARCHAR(4) PRIMARY KEY,
        venue_name VARCHAR(20),
        date_start DATETIME,
        date_end DATETIME
    );      
'''

