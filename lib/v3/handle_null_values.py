import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from logs import logger
from pyspark.sql.functions import isnan, when, count, col, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType
import datawig
import datetime





class handle_null_values(object):
    def __init__(self):
        self.variables=[]
        self.categorical_variables=[]
        self.numerical_variables=[]
        self.models={}
        self.df=None
              

    def get_variables_types(self):
        """numeric variables"""
        variables=self.df.dtypes
        self.variables= variables

        return variables
    
    
    def delete_var_with_null_more_than(self, dataframe=None, percentage=30):
        """numeric variables"""
        
        if dataframe == None:
            dataframe= self.df
            
        total_rows= dataframe.count()
        all_columns= dataframe.columns

        dropped_columns = []

        describe_dataframe= dataframe.select([count(when(col(c).isNull() | col(c).contains("NULL") | col(c).contains("null") | col(c).contains("None") | col(c).contains("NONE") | col(c).contains("none"), c)).alias(c) for c in dataframe.columns]).toPandas()

        for c in all_columns:
            missing_values= int(describe_dataframe[c][0])

            percent_missing= (missing_values)/total_rows *100

            if percent_missing>percentage:
                dropped_columns.append(c)

        return dropped_columns