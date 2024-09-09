from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, ltrim
#create silver clean classs
class Silverlayer:
    #init
    def __init__(self, df: DataFrame, 
                 dropna_columns: list = None,
                 columns_dropDuplicates: list = None,
                 columns_drop: list = None, 
                 columns_rename: dict = None, 
                 nested_columns: list = None,
                 missval_columns: dict = None,
                 ):
        '''
            Initializes processing task
        '''
        #firstly, check all types of parameters 
        if df is not None and not isinstance(df, DataFrame):
            raise TypeError("data must be a DataFrame")
        if columns_dropDuplicates is not None and not isinstance(columns_dropDuplicates, list):
            raise TypeError("columns_dropDuplicates must be a list")
        if columns_drop is not None and not isinstance(columns_drop, list):
            raise TypeError("columns_drop must be a list")
        if columns_rename is not None and not isinstance(columns_rename, dict):
            raise TypeError("columns_rename must be a dict")
        if nested_columns is not None and not isinstance(nested_columns, list):
            raise TypeError("nested_columns must be a list")
        if missval_columns is not None and not isinstance(missval_columns, dict):
            raise TypeError("columns_null must be a dict")
        if dropna_columns is not None and not isinstance(dropna_columns, list):
            raise TypeError("dropna_columns must be a list")
        #set value for class attrs
        #data frame
        self.df = df
        #list of columns to apply the drop duplicate function
        self.columns_dropDuplicates = columns_dropDuplicates
        #list of columns to drop
        self.columns_drop = columns_drop
        #dict containing old name & new name
        self.columns_rename = columns_rename
        #list of columns that need to handle nested structures
        self.nested_columns = nested_columns
        #dict containing columns to check & a value to apply for nulls
        self.missval_columns = missval_columns 
        #list of columns to apply drop na function
        self.dropna_columns = dropna_columns
    def drop_na(self, df: DataFrame, dropna_columns: list):
        '''
            Drop rows containing na values
        '''
        self.df = df.dropna(how = 'all', subset = dropna_columns)

    def drop_duplicates(self, df: DataFrame, columns_dropDuplicates: list):
        '''
            Drop duplicates based on specified columns
        '''
        self.df = df.dropDuplicates(columns_dropDuplicates)

    def drop_columns(self, df: DataFrame, columns_drop: list):
        '''
            Drop unnecessary columns
        '''
        self.df = df.drop(*columns_drop)

    def rename_columns(self, columns_rename: dict):
        '''
            Rename columns
        '''
        for old_name, new_name in columns_rename.items():
            self.df = self.df.withColumnRenamed(old_name, new_name)

    def handle_nested(self, nested_columns: list):
        '''
            Handle nested columns 
        '''
        for col in nested_columns:
            self.df = self.df.withColumn(col, explode_outer(col))
            self.df = self.df.withColumn(col, ltrim(col))

    def handle_missing(self, missval_columns: dict):
        '''
            Handle missing values
        '''
        for col, value in missval_columns.items():
            self.df = self.df.fillna(value = value, subset = col)

    def process(self) -> DataFrame:
        '''
            Process based on all parameters
        '''
        self.drop_na(self.df, self.dropna_columns)
        self.drop_duplicates(self.df, self.columns_dropDuplicates)

        if self.columns_drop:
            self.drop_columns(self.df, self.columns_drop)
        
        if self.columns_rename:
            self.rename_columns(self.columns_rename)

        if self.nested_columns:
            self.handle_nested(self.nested_columns)
        
        if self.missval_columns:
            self.handle_missing(self.missval_columns)

        return self.df