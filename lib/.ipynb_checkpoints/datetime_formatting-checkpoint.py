import re
import datetime
import pyspark.sql.functions as funct
from pyspark.sql.types import TimestampType
from dateutil import parser
from lib.logs import logger
import pyspark.sql.functions as funct


"""
1. Find which timestamp variables are saved as string
2. Get the format in which the time is saved as string.
3. Correct the time using Spark API
"""

class DatetimeFormatting_v2(object):
    def __init__(self):
        pass


    def run(self,df):
        
        try: 
            time_variables= self.find_time_variables(df)
            for v in time_variables:
                df= self.string_to_timestamp(df, v)
                self.update_metadata(v)
            return df
                
        except Exception as e:
            logger.error(e)
              

    def find_time_variables(self,df):
        variables= df.dtypes
        
        time_variables=[]
        for v in variables:
            if v[1]=="String":
                if False:         # add condition to check which string variable is actually a time variable and what is the format.
                    time_variables.append(v)
        
        return time_variables
    
    
    def string_to_timestamp(self, df, v):

        
        return df

    def update_metadata(self,column_name=[]):
        
        # update meta data
        
        return True
    
    
    


class DatetimeFormatting(object):
    def __init__(self):
        pass

    def date_formatting(self, x):
        return str(parser.parse(x))

    def udf_date_formatting(self):
        return funct.udf(lambda row: self.date_formatting(row))

    def date_cleaning(self, df, column_name=[]):
        try:
            for i in column_name:
                df = df.withColumn(i + '_new', self.udf_date_formatting()(funct.col(i).cast("String")))

            return df
        except Exception as e:
            logger.error(e)

    @staticmethod
    def fetch_columns_containing_datetime(df):

        try:
            col_dict = df.select([funct.col(col).rlike(r'(\d+(/|-){1}\d+(/|-){1}\d{2,4})').alias(col) for col in
                                  df.columns]).collect()[0].asDict()
            col_containig_url = [k for k, v in col_dict.items() if v is True]
            return col_containig_url
        except Exception as e:
            logger.error(e)

    



            
            
            
